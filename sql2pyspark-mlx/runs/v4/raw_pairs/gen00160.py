from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 517, "high").when(F.col("duration_sec") >= 156, "mid").otherwise("low").alias("duration_sec_bucket"),
        F.date_format("event_time", "yyyy-MM").alias("event_month"),
    )
    .agg(
        F.count("*").alias("web_event_count"),
        F.max("event_time").alias("last_event_time"),
    )
    .orderBy("duration_sec_bucket", "event_month")
    .limit(5)
)
