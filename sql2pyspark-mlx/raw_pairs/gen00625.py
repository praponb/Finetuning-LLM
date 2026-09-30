from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("id", "event_type")
    .exceptAll(
        df_web_events
        .filter(F.col("session_id").isin([3, 31, 36, 39]))
        .select("id", "event_type")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "page",
        F.when(F.col("duration_sec") >= 462, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
        F.min("event_time").alias("started_at"),
        F.countDistinct("page").alias("distinct_pages"),
    )
)
