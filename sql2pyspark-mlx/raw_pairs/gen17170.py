from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-06-23").cast("date")) & F.col("page").isNotNull())
    .select(
        "*",
        F.concat_ws("/", "page", "event_type").alias("page_event_type_key"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("duration_sec") >= 363, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
    )
    .orderBy(F.col("event_count").desc())
)
