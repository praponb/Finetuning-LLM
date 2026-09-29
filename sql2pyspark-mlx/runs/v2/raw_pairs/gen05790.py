from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([10, 15, 25]) & (F.col("event_time") < "2025-04-01") & (F.col("event_type") == "purchase"))
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 487, "high").when(F.col("duration_sec") >= 212, "mid").otherwise("low").alias("duration_sec_bucket"),
        "session_id",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy(F.col("session_id").desc(), "event_type", "duration_sec_bucket")
    .limit(5)
)
