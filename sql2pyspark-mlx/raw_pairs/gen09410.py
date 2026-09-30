from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 236)
    .select("id", "session_id", "page", "event_time")
    .orderBy("id")
)
