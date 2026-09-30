from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter(F.hour("event_time").between(10, 18) & ((F.col("event_type") == "purchase") | F.col("session_id").isin([25, 38])))
)

duration_sec_bucket_summary = (
    recent_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 396, "high").when(F.col("duration_sec") >= 179, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
)

result = (
    duration_sec_bucket_summary
    .filter(F.col("avg_duration_sec") < 450)
    .select("duration_sec_bucket", "page_view_count")
    .orderBy("duration_sec_bucket")
    .limit(3)
)
