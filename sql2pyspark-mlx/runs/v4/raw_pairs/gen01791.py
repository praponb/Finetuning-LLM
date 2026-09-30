from pyspark.sql import functions as F

lines = df_app_logs.select("id", "host", F.explode("context").alias("key", "value"))

cleaned = (
    lines
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("host")).alias("host_title"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled"),
    )
)

valid = (
    cleaned
    .filter(F.col("host") == "h2")
    .select(
        "*",
        F.trim("key").alias("clean_key"),
        F.when(F.col("host") == "h4", 1).otherwise(0).alias("is_h4"),
    )
)

matched_keys = (
    valid
    .select("id", "host")
    .intersect(
        df_app_logs
        .filter(F.col("latency_ms") != 922)
        .select("id", "host")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

stats = (
    kept_rows
    .groupBy(
        F.when(F.col("is_h4") >= 63, "PREMIUM").otherwise("STANDARD").alias("is_h4_tier"),
    )
    .agg(
        F.avg("is_h4").alias("avg_is_h4"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_count"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        F.round(F.avg("is_h4"), 2).alias("avg_is_h4_2"),
    )
    .filter(F.col("avg_is_h4") >= 0)
)

result = (
    stats
    .select("is_h4_tier", "h1_count")
    .orderBy(F.col("is_h4_tier").desc_nulls_first(), "h1_count")
    .limit(5)
)
