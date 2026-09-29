from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id", "session_id", "event_time")
    .orderBy(F.col("id").desc())
)
