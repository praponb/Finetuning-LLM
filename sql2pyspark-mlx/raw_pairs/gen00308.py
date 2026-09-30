from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .select("duration_sec", "session_id", "event_type")
)
