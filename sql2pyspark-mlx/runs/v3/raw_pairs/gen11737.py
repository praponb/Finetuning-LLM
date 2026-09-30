from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") >= 167) | (F.col("event_time") >= "2024-03-01"))
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .filter(F.col("total_duration_sec") > 2100)
    .orderBy(F.col("event_hour").desc())
)
