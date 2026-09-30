from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("region_rows").desc())

lines = (
    df_app_logs
    .select("id", "service", "level", F.explode_outer("context").alias("key", "v"))
)

base = (
    lines
    .filter(F.col("key").isNotNull())
    .withColumn("key_prefix", F.substring("key", 1, 2))
    .withColumn("key_clean", F.coalesce("key", F.lit("OTHER")))
)

cleaned = (
    base
    .filter(F.col("key").isNotNull() & (F.col("key") == "version"))
    .select(
        "*",
        F.lower("service").alias("service_lower"),
        F.lower("level").alias("level_lower"),
    )
)

filtered = (
    cleaned
    .filter(F.col("key").isNotNull() & ~F.col("key").isin(["trace", "region"]))
    .select(
        "id",
        "service",
        "level",
        "key",
        "service_lower",
        F.trim("service").alias("clean_service"),
        F.trim("level").alias("clean_level"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("key").isNotNull())
    .withColumn("service_upper", F.upper("service"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("OTHER")))
)

filtered_app_logs_2 = (
    filtered_app_logs
    .filter(F.col("key").isNotNull())
    .select(
        "key",
        "service_lower",
        "clean_service",
        "service_upper",
        "resolved_key",
        F.initcap(F.trim("level")).alias("level_title"),
        F.upper("level").alias("level_upper"),
    )
)

cleaned_app_logs = (
    filtered_app_logs_2
    .filter(F.col("key").isNotNull())
    .select(
        "key",
        "service_lower",
        "clean_service",
        "service_upper",
        "resolved_key",
        "level_title",
        "level_upper",
        F.length("key").alias("key_length"),
    )
)

totals = (
    cleaned_app_logs
    .groupBy(
        "key",
        F.when(F.col("key_length") >= 81, "PREMIUM").otherwise("STANDARD").alias("key_length_tier"),
    )
    .agg(
        F.round(F.sum("key_length") / F.nullif(F.count(F.when(F.col("key") == "region", 1)), F.lit(0)), 2).alias("key_length_per_region"),
        F.round(F.avg("key_length"), 2).alias("avg_key_length"),
        F.countDistinct("key").alias("distinct_keys"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_rows"),
        F.sum(F.when(F.col("key") == "region", F.col("key_length")).otherwise(0)).alias("region_key_length"),
    )
    .withColumn("region_rows_rank", F.rank().over(w))
)

result = (
    totals
    .select(
        "key",
        "key_length_tier",
        "key_length_per_region",
        "avg_key_length",
        "distinct_keys",
        "region_key_length",
    )
)
