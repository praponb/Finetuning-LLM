from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .orderBy("min_duration_sec", F.col("avg_duration_sec").desc())
)
