from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", "event_type", "session_id")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)
