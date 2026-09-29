from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2025-05-01") & (F.col("event_time") < "2023-10-24"))
    .select("id", "event_type", "session_id", "page")
)
