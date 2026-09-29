from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
