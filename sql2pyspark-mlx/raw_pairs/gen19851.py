from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy(F.year("effective_from"), "currency").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value_rank_2").asc_nulls_last())
w_part = Window.orderBy(F.col("tier_value_rank_2_share").asc_nulls_first())
w6 = Window.partitionBy("currency").orderBy(F.col("tier_value_rank_2_share").desc_nulls_first())
w7 = Window.partitionBy("currency").orderBy(F.col("tier_value_rank_2_cume_dist_int").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

with_windows = (
    lines
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("tier_value_rank_2", F.rank().over(w2))
)

enriched = (
    with_windows
    .filter(F.col("tier_value") <= 22)
    .select(
        "id",
        "currency",
        "tier_value_rank_2",
        F.round((F.col("tier_value_rank_2") / F.sum("tier_value_rank_2").over(w3)), 4).alias("tier_value_rank_2_share"),
        F.cume_dist().over(w4).alias("tier_value_rank_2_cume_dist"),
    )
)

base = (
    enriched
    .filter(F.col("currency").isin(["EUR", "THB"]))
    .select(
        "id",
        "currency",
        "tier_value_rank_2_share",
        "tier_value_rank_2_cume_dist",
        F.length("currency").alias("currency_length"),
    )
)

cleaned = (
    base
    .filter(F.col("currency_length") <= 56)
    .select(
        "id",
        "currency",
        "tier_value_rank_2_share",
        "tier_value_rank_2_cume_dist",
        "currency_length",
        F.col("tier_value_rank_2_cume_dist").cast("int").alias("tier_value_rank_2_cume_dist_int"),
    )
)

with_windows_price_tiers = (
    cleaned
    .withColumn("currency_sum_tier_value_rank_2_cume_dist_int", F.sum("tier_value_rank_2_cume_dist_int").over(w3))
    .withColumn("tier_value_rank_2_share_cume_dist", F.cume_dist().over(w_part))
    .withColumn("tier_value_rank_2_share_rank", F.rank().over(w6))
    .withColumn("tier_value_rank_2_cume_dist_int_dense_rank", F.dense_rank().over(w7))
    .withColumn("currency_min_currency_length", F.min("currency_length").over(w3))
)

result = (
    with_windows_price_tiers
    .select(
        "id",
        "currency",
        "tier_value_rank_2_share",
        "tier_value_rank_2_cume_dist",
        "currency_length",
        "tier_value_rank_2_cume_dist_int",
        "currency_sum_tier_value_rank_2_cume_dist_int",
        "tier_value_rank_2_share_cume_dist",
        "tier_value_rank_2_cume_dist_int_dense_rank",
    )
    .orderBy("currency_length", F.col("currency").desc())
)
