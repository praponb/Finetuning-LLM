from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_info_rounded")
w2 = Window.partitionBy("host")
w3 = Window.orderBy("status_code")
w4 = Window.orderBy(F.col("is_info_rounded").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

valid = (
    flattened
    .filter(F.col("attr_key").isNotNull())
    .withColumn("is_info", F.when(F.col("level") == "INFO", 1).otherwise(0))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

filtered = (
    valid
    .filter(F.col("host") == "h2")
    .select(
        "*",
        F.round("is_info", 0).alias("is_info_rounded"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("attr_key_filled"),
    )
)

enriched = (
    filtered
    .filter(F.col("is_info_rounded") >= 29)
    .select(
        "id",
        "service",
        "host",
        "level",
        "is_info",
        "attr_key_clean",
        F.dense_rank().over(w).alias("is_info_rounded_dense_rank"),
        F.avg("is_info_rounded").over(w2).alias("host_avg_is_info_rounded"),
        F.rank().over(w3).alias("status_code_rank"),
        F.dense_rank().over(w4).alias("is_info_rounded_dense_rank_2"),
    )
)

totals = (
    enriched
    .groupBy(
        F.when(F.col("status_code_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("status_code_rank_tier"),
    )
    .agg(
        F.sum(F.when(F.col("service") == "checkout", F.col("status_code_rank")).otherwise(0)).alias("checkout_status_code_rank"),
        F.round(F.sum("is_info_rounded_dense_rank_2") / F.nullif(F.count(F.when(F.col("service") == "billing", 1)), F.lit(0)), 2).alias("is_info_rounded_dense_rank_2_per_billing"),
        F.round(F.avg("is_info_rounded_dense_rank_2"), 2).alias("avg_is_info_rounded_dense_rank_2"),
        F.sum(F.when(F.col("level") == "INFO", F.col("host_avg_is_info_rounded")).otherwise(0)).alias("info_host_avg_is_info_rounded"),
        F.percentile_approx("is_info_rounded_dense_rank", 0.95).alias("is_info_rounded_dense_rank_p95"),
    )
    .filter(F.col("info_host_avg_is_info_rounded") < 1000000)
)

result = (
    totals
    .select(
        "status_code_rank_tier",
        "checkout_status_code_rank",
        "is_info_rounded_dense_rank_2_per_billing",
        "info_host_avg_is_info_rounded",
        "is_info_rounded_dense_rank_p95",
    )
    .orderBy(F.col("is_info_rounded_dense_rank_p95").desc_nulls_first())
)
