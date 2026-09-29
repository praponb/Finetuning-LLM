from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .filter(F.col("min_duration_sec") > 410)
    .select("event_month", "stddev_duration_sec", "min_duration_sec")
    .orderBy("stddev_duration_sec", F.col("event_month").desc())
)
