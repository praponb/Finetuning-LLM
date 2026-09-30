from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "page_view") & (F.col("session_id").isin([15, 32, 35]) | (F.col("event_time") >= "2024-05-01")))
    .select("id", "duration_sec", "session_id", "event_type")
    .orderBy("event_type")
)
