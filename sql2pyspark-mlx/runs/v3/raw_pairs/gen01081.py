from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "page",
        F.when(F.col("duration_sec") >= 422, "high").when(F.col("duration_sec") >= 101, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.countDistinct("duration_sec").alias("unique_pages"),
    )
    .filter(F.col("unique_pages") > 9)
    .select("page", "duration_sec_bucket", "latest_event_time", "unique_pages")
)
