from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 25) & (F.col("event_type") != "page_view"))
    .select("id", "duration_sec", "event_time")
    .orderBy("duration_sec")
)
