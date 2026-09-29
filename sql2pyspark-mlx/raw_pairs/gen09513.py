from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_prices").alias("tier_value"))
)

with_windows = (
    lines
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w_ordered))
)

valid = (
    with_windows
    .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
    .select(
        "currency",
        "tier_value_dense_rank_2",
        F.round((F.col("tier_value_dense_rank") / F.col("tier_value_dense_rank_2")), 2).alias("tier_value_dense_rank_per_tier_value_dense_rank_2"),
    )
)

base = (
    valid
    .filter(F.col("currency").isin(["THB", "USD"]))
    .withColumn("currency_title", F.initcap(F.trim("currency")))
)

with_windows_price_tiers = (
    base
    .select(
        "currency",
        "tier_value_dense_rank_2",
        "tier_value_dense_rank_per_tier_value_dense_rank_2",
        F.round((F.col("tier_value_dense_rank_2") / F.sum("tier_value_dense_rank_2").over(w2)), 4).alias("tier_value_dense_rank_2_share"),
        F.round((F.col("tier_value_dense_rank_2") / F.sum("tier_value_dense_rank_2").over(w2)), 4).alias("tier_value_dense_rank_2_share_2"),
    )
)

result = (
    with_windows_price_tiers
    .select(
        "currency",
        "tier_value_dense_rank_2",
        "tier_value_dense_rank_per_tier_value_dense_rank_2",
    )
)
