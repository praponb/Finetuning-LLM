from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.year("event_time").alias("event_year"), "event_type")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
    .orderBy(F.col("stddev_duration_sec").desc())
)
