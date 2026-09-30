from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id", "page")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") >= 460)
)
