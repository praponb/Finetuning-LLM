from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .withColumn("duration_sec_filled", F.coalesce("duration_sec", F.lit(0)))
)

summary = (
    filtered_web_events
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 415, "high").when(F.col("duration_sec") >= 181, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.sum("duration_sec_filled").alias("total_duration_sec_filled"),
        F.min("event_time").alias("min_event_time"),
    )
)

result = (
    summary
    .filter(F.col("total_duration_sec_filled") < 420)
    .select(
        "event_type",
        "duration_sec_bucket",
        "total_duration_sec_filled",
        "min_event_time",
    )
    .orderBy("duration_sec_bucket")
)
