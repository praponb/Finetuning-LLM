from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "purchase"]) & ((F.col("event_time") < "2024-06-01") | F.col("duration_sec").between(236, 355)))
)

web_events_stats = (
    filtered_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 435, "high").when(F.col("duration_sec") >= 158, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    web_events_stats
    .select("duration_sec_bucket", "duration_sec_range")
    .orderBy("duration_sec_bucket")
)
