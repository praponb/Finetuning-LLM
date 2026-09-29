from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([9, 30, 40]) & (F.col("event_time") >= "2025-09-01"))
    .select("event_time")
)
