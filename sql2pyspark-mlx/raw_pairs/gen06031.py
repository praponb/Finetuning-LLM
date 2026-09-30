from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 205)
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_pages"),
    )
    .filter(F.col("stddev_duration_sec") >= 79)
)
