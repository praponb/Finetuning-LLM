from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value_percent_rank").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

enriched = (
    lines
    .select(
        "*",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.stddev_samp("tier_value").over(w2).alias("currency_stddev_samp_tier_value"),
    )
)

cleaned = (
    enriched
    .filter(F.col("currency") == "EUR")
    .select(
        "id",
        "currency",
        "tier_value",
        "tier_value_percent_rank",
        F.col("tier_value_percent_rank").cast("int").alias("tier_value_percent_rank_int"),
    )
)

metrics = (
    cleaned
    .withColumn("tier_value_percent_rank_cume_dist", F.cume_dist().over(w3))
    .withColumn("currency_max_tier_value", F.max("tier_value").over(w2))
    .withColumn("currency_count_tier_value", F.count("tier_value").over(w2))
)

result = (
    metrics
    .groupBy("currency")
    .agg(
        F.round(F.sum("tier_value_percent_rank_cume_dist") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("tier_value_percent_rank_cume_dist_per_thb"),
        F.percentile_approx("currency_max_tier_value", 0.5).alias("currency_max_tier_value_p50"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.round(F.avg("currency_count_tier_value"), 2).alias("avg_currency_count_tier_value"),
    )
    .filter(F.col("tier_value_percent_rank_cume_dist_per_thb") >= 0)
    .orderBy(F.col("currency").asc_nulls_last())
)
