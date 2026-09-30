from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .groupBy(
        F.quarter("event_time").alias("event_quarter"),
        F.hour("event_time").alias("event_hour"),
    )
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)
