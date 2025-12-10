from fastapi import APIRouter, File, UploadFile, HTTPException, Query, BackgroundTasks
from app.models import TranscriptionResponse, FetchAndTranscribeRequest
from app.services import transcriber_service
from app.asr_providers import ASRFactory, CURRENT_PROVIDER, CURRENT_MODEL
from typing import Optional
import time
import logging
import json
import boto3
import os
from pydantic import BaseModel

# ロギング設定
logger = logging.getLogger(__name__)

# AWS SQS client
sqs = boto3.client('sqs', region_name='ap-southeast-2')
FEATURE_COMPLETED_QUEUE_URL = os.environ.get(
    'FEATURE_COMPLETED_QUEUE_URL',
    'https://sqs.ap-southeast-2.amazonaws.com/754724220380/watchme-feature-completed-queue'
)

router = APIRouter()

# Request model for async processing
class AsyncProcessRequest(BaseModel):
    file_path: str
    device_id: str
    recorded_at: str

@router.post("/analyze/azure", response_model=TranscriptionResponse)
async def analyze_audio(
    file: UploadFile = File(...),
    detailed: bool = Query(False, description="詳細な結果（信頼度、統計情報）を取得"),
    high_accuracy: bool = Query(False, description="高精度モード（時間がかかりますが精度が向上）"),
    provider: Optional[str] = Query(None, description="ASRプロバイダー指定（azure, groq, deepgram, aiola）※テスト用"),
    model: Optional[str] = Query(None, description="モデル指定※テスト用")
):
    """ASRプロバイダーを使用して音声ファイルを文字起こしする

    ※ provider/modelパラメータを指定することで、デプロイせずに複数プロバイダーをテスト可能
    """

    # ファイル形式チェック
    allowed_extensions = ['.wav', '.mp3', '.m4a']
    file_extension = None

    if file.filename:
        file_extension = '.' + file.filename.split('.')[-1].lower()
        if file_extension not in allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"サポートされていないファイル形式です。対応形式: {', '.join(allowed_extensions)}"
            )
    else:
        raise HTTPException(status_code=400, detail="ファイル名が指定されていません")

    # ファイルサイズチェック（25MB制限）
    if file.size and file.size > 25 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="ファイルサイズが25MBを超えています")

    try:
        # プロバイダーの選択（動的またはデフォルト）
        if provider:
            # クエリパラメータで指定された場合は動的に生成
            logger.info(f"🔄 動的プロバイダー切り替え: {provider}/{model or 'default'}")
            asr_provider = ASRFactory.create(provider, model)
        else:
            # デフォルトプロバイダーを使用
            asr_provider = transcriber_service.asr_provider

        # 音声文字起こし実行
        result = await asr_provider.transcribe_audio(
            file.file,
            file.filename,
            detailed=detailed,
            high_accuracy=high_accuracy
        )

        # プロバイダー情報をレスポンスに追加
        result['asr_provider'] = asr_provider.provider_name
        result['asr_model'] = asr_provider.model_name

        return TranscriptionResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"予期しないエラーが発生しました: {str(e)}")

@router.post("/async-process", status_code=202)
async def async_process(
    request: AsyncProcessRequest,
    background_tasks: BackgroundTasks
):
    """
    Asynchronous processing endpoint - returns 202 Accepted immediately
    and processes in the background
    """
    logger.info(f"Starting async processing for {request.device_id} at {request.recorded_at}")

    # Update status to 'processing' in database
    try:
        await transcriber_service.update_status(
            request.device_id,
            request.recorded_at,
            "vibe_status",
            "processing"
        )
    except Exception as e:
        logger.error(f"Failed to update status: {e}")

    # Add to background tasks
    background_tasks.add_task(
        process_in_background,
        request.file_path,
        request.device_id,
        request.recorded_at
    )

    return {
        "status": "accepted",
        "message": "Processing started in background",
        "device_id": request.device_id,
        "recorded_at": request.recorded_at
    }


async def process_in_background(file_path: str, device_id: str, recorded_at: str):
    """
    Background processing function - runs after returning 202 to client
    """
    logger.info(f"Background processing started for {device_id}")

    try:
        # Create request for existing service
        request = FetchAndTranscribeRequest(
            file_paths=[file_path],
            model="groq"  # Use Groq by default for faster processing
        )

        # Call existing processing function
        result = await transcriber_service.fetch_and_transcribe_files(request)

        # Update status to 'completed'
        await transcriber_service.update_status(
            device_id,
            recorded_at,
            "vibe_status",
            "completed"
        )

        # Send completion notification to SQS
        sqs.send_message(
            QueueUrl=FEATURE_COMPLETED_QUEUE_URL,
            MessageBody=json.dumps({
                "device_id": device_id,
                "recorded_at": recorded_at,
                "feature_type": "vibe",
                "status": "completed",
                "processed_files": result.get('processed_files', [])
            })
        )

        logger.info(f"Background processing completed for {device_id}")

    except Exception as e:
        logger.error(f"Background processing failed for {device_id}: {str(e)}")

        # Update status to 'failed'
        try:
            await transcriber_service.update_status(
                device_id,
                recorded_at,
                "vibe_status",
                "failed"
            )
        except:
            pass

        # Send failure notification to SQS
        sqs.send_message(
            QueueUrl=FEATURE_COMPLETED_QUEUE_URL,
            MessageBody=json.dumps({
                "device_id": device_id,
                "recorded_at": recorded_at,
                "feature_type": "vibe",
                "status": "failed",
                "error": str(e)
            })
        )


@router.post("/fetch-and-transcribe")
async def fetch_and_transcribe(request: FetchAndTranscribeRequest):
    """WatchMeシステムのメイン処理エンドポイント（マルチプロバイダー対応）"""
    start_time = time.time()

    # サポートされているモデルの確認（後方互換性のため "azure" も受け入れる）
    if request.model not in ["azure", "groq"]:
        raise HTTPException(
            status_code=400,
            detail=f"サポートされていないモデル: {request.model}. 対応モデル: azure, groq"
        )

    # ASR処理を実行
    try:
        result = await transcriber_service.fetch_and_transcribe_files(request)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"fetch_and_transcribe エラー: {str(e)}")
        raise HTTPException(status_code=500, detail=f"予期しないエラーが発生しました: {str(e)}") 