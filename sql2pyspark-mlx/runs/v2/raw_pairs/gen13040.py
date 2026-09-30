from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/search") & ((F.col("session_id") == 36) | (F.to_date("event_time") >= "2025-01-01")))
    .select("id", "session_id", "page", "event_time")
)
