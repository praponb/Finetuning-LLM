from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec").between(243, 276) & (F.col("event_time") < "2024-05-01")) | (F.col("event_type") == "click"))
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.max("event_time").alias("latest_event_time"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("stddev_duration_sec") > 44)
)
