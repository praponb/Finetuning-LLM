from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.max("event_time").alias("latest_event_time"),
    )
    .filter(F.col("unique_pages") >= 5)
    .orderBy("unique_pages")
)
