from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("duration_sec") > 301)
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.when(F.col("event_type").isin(["purchase", "click"]), "Group A").otherwise("Other").alias("event_type_group"),
        F.when(F.col("duration_sec") >= 425, "High").when(F.col("duration_sec") >= 163, "Medium").otherwise("Low").alias("duration_sec_band"),
    )
)

result = (
    filtered
    .groupBy(
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        F.when(F.col("duration_sec") >= 466, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.max("event_time").alias("last_seen"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
    )
    .filter(F.col("distinct_session_ids") > 1)
)
