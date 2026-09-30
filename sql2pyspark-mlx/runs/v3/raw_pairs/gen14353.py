from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") >= 249)
    .groupBy("session_id")
    .agg(F.min("event_time").alias("first_event_time"))
)
