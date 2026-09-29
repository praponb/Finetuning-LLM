from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter((F.col("session_id") == 1) & (F.col("event_time") < "2025-07-13"))
    .withColumn("page_prefix", F.substring("page", 1, 3))
)

summary = (
    valid_web_events
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    summary
    .filter(F.col("pct_search") >= 75)
    .select("event_hour", "pct_search", "min_duration_sec")
    .orderBy(F.col("min_duration_sec").desc())
)
