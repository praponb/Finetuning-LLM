from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("currency_min_tier_value").asc_nulls_last())
w4 = Window.orderBy(F.col("tier_value_percent_rank").desc())
w5 = Window.orderBy(F.col("currency_stddev_samp_tier_value").desc_nulls_first())
w6 = Window.orderBy(F.col("currency_min_tier_value").desc_nulls_first())
w7 = Window.partitionBy("currency").orderBy(F.col("tier_value_percent_rank_dense_rank").desc_nulls_first())
w_running = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w9 = Window.partitionBy("currency").orderBy(F.col("currency_min_tier_value").desc())
w10 = Window.partitionBy("currency").orderBy(F.col("currency_stddev_samp_tier_value_percent_rank").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("tier_value"))
)

valid = (
    exploded
    .filter(F.col("currency").isin(["THB", "USD", "EUR"]))
    .select(
        "*",
        F.when(F.col("tier_value") >= 73, "High").when(F.col("tier_value") >= 15, "Medium").otherwise("Low").alias("tier_value_band"),
    )
)

with_windows = (
    valid
    .withColumn("currency_min_tier_value", F.min("tier_value").over(w))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w2))
    .withColumn("currency_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w))
)

scored = (
    with_windows
    .select(
        "id",
        "currency",
        "tier_value",
        "tier_value_band",
        "currency_min_tier_value",
        "currency_stddev_samp_tier_value",
        F.percent_rank().over(w3).alias("currency_min_tier_value_percent_rank"),
        F.dense_rank().over(w4).alias("tier_value_percent_rank_dense_rank"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "currency",
        "tier_value",
        "tier_value_band",
        "currency_min_tier_value",
        "currency_min_tier_value_percent_rank",
        F.percent_rank().over(w5).alias("currency_stddev_samp_tier_value_percent_rank"),
        F.cume_dist().over(w6).alias("currency_min_tier_value_cume_dist"),
        F.dense_rank().over(w7).alias("tier_value_percent_rank_dense_rank_dense_rank"),
    )
)

enriched_price_tiers = (
    enriched
    .select(
        "tier_value",
        "currency_min_tier_value",
        "currency_min_tier_value_cume_dist",
        F.count("tier_value").over(w).alias("currency_count_tier_value"),
        F.avg("currency_min_tier_value").over(w).alias("currency_avg_currency_min_tier_value"),
        F.percent_rank().over(w_running).alias("tier_value_percent_rank"),
        F.percent_rank().over(w9).alias("currency_min_tier_value_percent_rank_2"),
        F.cume_dist().over(w10).alias("currency_stddev_samp_tier_value_percent_rank_cume_dist"),
    )
)

result = (
    enriched_price_tiers
    .select(
        "tier_value",
        "currency_min_tier_value",
        "currency_min_tier_value_cume_dist",
        "currency_avg_currency_min_tier_value",
        "currency_min_tier_value_percent_rank_2",
    )
    .orderBy(F.col("tier_value").desc_nulls_last())
)
