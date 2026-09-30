from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 17)
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"), "page")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
    .filter(F.col("stddev_duration_sec") >= 37)
    .orderBy(F.col("unique_sessions").desc())
)
