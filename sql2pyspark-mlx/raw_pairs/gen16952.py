from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~((F.col("event_time") >= "2024-05-01") & (F.col("event_time") >= "2024-06-26")))
    .select("event_type", "session_id", "event_time")
    .orderBy(F.col("event_type").desc())
)
