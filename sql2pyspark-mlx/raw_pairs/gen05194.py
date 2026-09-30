from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.orderBy(F.col("currency_stddev_samp_tier_value").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("currency_stddev_samp_tier_value").desc())
w5 = Window.orderBy(F.col("currency_stddev_samp_tier_value").desc())

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("discounts").alias("tier_value"))
)

remaining_keys = (
    flattened
    .select("currency", "id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan").isin(["starter", "pro", "enterprise"]))
        .select("currency", "id")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.stddev_samp("tier_value").over(w).alias("currency_stddev_samp_tier_value"),
        F.rank().over(w2).alias("tier_value_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("tier_value") <= 40)
    .withColumn("tier_value_whole", F.floor("tier_value"))
)

with_windows = (
    valid
    .filter(F.col("tier_value_rank") <= 8)
    .select(
        "id",
        "currency",
        "currency_stddev_samp_tier_value",
        F.dense_rank().over(w3).alias("currency_stddev_samp_tier_value_dense_rank"),
        F.min("tier_value").over(w).alias("currency_min_tier_value"),
        F.rank().over(w4).alias("currency_stddev_samp_tier_value_rank"),
        F.sum("tier_value").over(w).alias("currency_sum_tier_value"),
    )
)

with_windows_price_tiers = (
    with_windows
    .select(
        "*",
        F.percent_rank().over(w5).alias("currency_stddev_samp_tier_value_percent_rank"),
    )
)

cleaned = (
    with_windows_price_tiers
    .filter(F.col("currency_stddev_samp_tier_value") == 39)
    .select(
        "currency_stddev_samp_tier_value",
        "currency_stddev_samp_tier_value_dense_rank",
        "currency_min_tier_value",
        "currency_stddev_samp_tier_value_rank",
        "currency_sum_tier_value",
        "currency_stddev_samp_tier_value_percent_rank",
        F.round("currency_sum_tier_value", 1).alias("currency_sum_tier_value_rounded"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("currency_min_tier_value") >= 85, "LARGE").otherwise("SMALL").alias("currency_min_tier_value_tier"),
        F.when(F.col("currency_sum_tier_value_rounded") >= 90, "LARGE").otherwise("SMALL").alias("currency_sum_tier_value_rounded_tier"),
    )
    .agg(
        F.max("currency_sum_tier_value_rounded").alias("max_currency_sum_tier_value_rounded"),
        F.percentile_approx("currency_stddev_samp_tier_value_percent_rank", [0.25, 0.5, 0.75]).alias("currency_stddev_samp_tier_value_percent_rank_quartiles"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("currency_sum_tier_value_rounded"), 2).alias("avg_currency_sum_tier_value_rounded"),
        F.percentile_approx("currency_stddev_samp_tier_value_dense_rank", [0.25, 0.5, 0.75]).alias("currency_stddev_samp_tier_value_dense_rank_quartiles"),
    )
    .orderBy(F.col("max_currency_sum_tier_value_rounded").desc_nulls_first())
)
