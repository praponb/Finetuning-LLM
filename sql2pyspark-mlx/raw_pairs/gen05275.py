from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "level")
w2 = Window.orderBy("status_code")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

valid = (
    lines
    .filter(~F.col("attr_key").eqNullSafe(F.lit("version")))
    .select("*", F.initcap(F.trim("attr_key")).alias("attr_key_title"))
)

filtered = (
    valid
    .filter(F.col("attr_key").isin(["version", "region", "trace"]))
    .select(
        "host",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "v",
        "attr_key_title",
        F.greatest("latency_ms", "status_code").alias("greatest_latency_ms_status_code"),
        F.when(F.col("attr_key") == "region", 1).otherwise(0).alias("is_region"),
    )
)

base = (
    filtered
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("version"))))
    .select(
        "host",
        "level",
        "status_code",
        "attr_key",
        "v",
        "attr_key_title",
        "is_region",
        F.lower("level").alias("level_lower"),
    )
)

enriched = (
    base
    .filter(F.col("attr_key").isin(["region", "trace", "version"]))
    .select(
        "*",
        F.min("status_code").over(w).alias("attr_key_min_status_code"),
        F.rank().over(w2).alias("status_code_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("is_region") >= 67, "PREMIUM").otherwise("STANDARD").alias("is_region_tier"),
        F.when(F.col("attr_key_min_status_code") >= 400, "HIGH").otherwise("LOW").alias("attr_key_min_status_code_tier"),
    )
    .agg(
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_rows"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
        F.sum(F.when(F.col("level") == "WARN", F.col("is_region")).otherwise(0)).alias("warn_is_region"),
        F.round(F.sum("is_region") / F.nullif(F.count(F.when(F.col("level") == "DEBUG", 1)), F.lit(0)), 2).alias("is_region_per_debug"),
    )
    .orderBy(F.col("warn_is_region").desc())
)
