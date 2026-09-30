from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.partitionBy("currency")
w4 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w5 = Window.partitionBy("plan")
w_part = Window.partitionBy("plan", "currency")

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("tier_value"))
)

with_windows = (
    flattened
    .filter(F.col("plan") != "pro")
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("currency_count_tier_value", F.count("tier_value").over(w3))
)

filtered = (
    with_windows
    .filter(F.col("plan").isin(["starter", "pro"]))
    .withColumn("tier_value_percent_rank_plus_currency_count_tier_value", (F.col("tier_value_percent_rank") + F.col("currency_count_tier_value")))
)

kept_keys = (
    filtered
    .select("id", "currency")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") != "USD")
        .select("id", "currency")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("tier_value").between(22, 26))
    .select(
        "plan",
        "currency",
        "tier_value",
        "tier_value_percent_rank",
        "tier_value_percent_rank_plus_currency_count_tier_value",
        F.concat_ws("-", "plan", "currency").alias("plan_currency_key"),
    )
)

with_windows_price_tiers = (
    cleaned
    .select(
        "currency",
        "plan_currency_key",
        F.cume_dist().over(w4).alias("tier_value_cume_dist"),
        F.max("tier_value_percent_rank_plus_currency_count_tier_value").over(w5).alias("plan_max_tier_value_percent_rank_plus_currency_count_tier_value"),
        F.stddev_samp("tier_value_percent_rank_plus_currency_count_tier_value").over(w_part).alias("plan_stddev_samp_tier_value_percent_rank_plus_currency_count_tier_value"),
        F.max("tier_value").over(w_part).alias("plan_max_tier_value"),
    )
)

summary = (
    with_windows_price_tiers
    .withColumn("tier_value_cume_dist_tier", F.when(F.col("tier_value_cume_dist") >= 0.61, "HIGH").otherwise("LOW"))
    .cube("currency", "tier_value_cume_dist_tier")
    .agg(
        F.round(F.avg("tier_value_cume_dist"), 2).alias("avg_tier_value_cume_dist"),
        F.round(F.sum("plan_max_tier_value_percent_rank_plus_currency_count_tier_value") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("plan_max_tier_value_percent_rank_plus_currency_count_tier_value_per_thb"),
        F.percentile_approx("tier_value_cume_dist", [0.25, 0.5, 0.75]).alias("tier_value_cume_dist_quartiles"),
        F.percentile_approx("plan_stddev_samp_tier_value_percent_rank_plus_currency_count_tier_value", 0.9).alias("plan_stddev_samp_tier_value_percent_rank_plus_currency_count_tier_value_p90"),
        F.round(F.avg("tier_value_cume_dist"), 2).alias("avg_tier_value_cume_dist_2"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.grouping("currency").alias("grouping_level"),
    )
    .filter(F.col("plan_stddev_samp_tier_value_percent_rank_plus_currency_count_tier_value_p90") >= 0)
)

result = (
    summary
    .select(
        "tier_value_cume_dist_tier",
        "avg_tier_value_cume_dist",
        "plan_max_tier_value_percent_rank_plus_currency_count_tier_value_per_thb",
        "tier_value_cume_dist_quartiles",
        "avg_tier_value_cume_dist_2",
        "grouping_level",
    )
)
