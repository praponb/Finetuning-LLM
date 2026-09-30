from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.to_date("event_time").alias("event_day"), "page")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .filter(F.col("total_duration_sec") >= 2400)
)
