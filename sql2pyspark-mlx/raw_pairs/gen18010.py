from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("duration_sec") > 217)
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.substring("event_type", 1, 3).alias("event_type_prefix"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("duration_sec") >= 439, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.min("event_time").alias("started_at"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.count("*").alias("cnt"),
    )
)
