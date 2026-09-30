from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 34)
    .select("id", "event_type", "event_time")
    .orderBy(F.col("id").desc())
    .limit(20)
)
