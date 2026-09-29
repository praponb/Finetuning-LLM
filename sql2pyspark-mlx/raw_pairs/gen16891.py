from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(305, 382) & (F.col("duration_sec") != 429))
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)
