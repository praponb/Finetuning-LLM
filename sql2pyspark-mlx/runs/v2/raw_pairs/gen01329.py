from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([12, 28]))
    .groupBy("event_type")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
    .orderBy("avg_duration_sec", "event_type")
    .limit(1)
)
