from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 436, "high").when(F.col("duration_sec") >= 212, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") > 450)
)
