from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("event_type") != "click") & F.hour("event_time").between(7, 12)) | F.col("page").isNotNull())
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.max("event_time").alias("latest_event_time"),
    )
    .filter(F.col("avg_duration_sec") >= 73)
)
