from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(12, 20))
    .groupBy("page", "event_type")
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") > 79)
)
