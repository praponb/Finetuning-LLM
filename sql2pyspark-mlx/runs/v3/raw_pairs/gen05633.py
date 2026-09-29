from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("page") == "/home") & (F.col("duration_sec") > 230)) | (F.to_date("event_time") >= "2025-04-01"))
    .select("id", "duration_sec", "session_id", "page")
)
