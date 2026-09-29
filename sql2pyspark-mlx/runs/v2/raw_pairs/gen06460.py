from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 523, "high").when(F.col("duration_sec") >= 131, "mid").otherwise("low").alias("duration_sec_bucket"),
        F.quarter("event_time").alias("event_quarter"),
    )
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .orderBy(F.col("min_duration_sec").desc(), "duration_sec_bucket", "event_quarter")
    .limit(10)
)
