from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull())
    .select("session_id", "event_type", "event_time")
)
