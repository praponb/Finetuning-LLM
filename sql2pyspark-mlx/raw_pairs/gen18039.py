from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .filter(F.col("unique_sessions") >= 4)
    .orderBy("unique_sessions", "event_type")
)
