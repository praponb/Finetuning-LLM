from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 257)
    .select("id", "event_type", "session_id", "event_time")
)
