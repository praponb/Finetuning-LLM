from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([35, 38]))
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .orderBy("avg_duration_sec")
)
