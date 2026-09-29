from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.hour("event_time").alias("event_hour"),
        F.date_trunc("MONTH", "event_time").alias("event_month"),
    )
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .filter(F.col("unique_sessions") > 5)
)
