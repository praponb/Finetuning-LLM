from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("tier_value"))
)

matched_keys = (
    flattened
    .select("currency", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "pro")
        .select("currency", "id")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "id",
        "currency",
        F.sum("tier_value").over(w).alias("currency_sum_tier_value"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
        F.rank().over(w3).alias("tier_value_rank"),
        F.dense_rank().over(w4).alias("tier_value_dense_rank_2"),
    )
)

valid = (
    metrics
    .filter(F.col("tier_value_dense_rank_2") != 5)
    .select(
        "id",
        "currency",
        "tier_value_dense_rank",
        "tier_value_rank",
        "tier_value_dense_rank_2",
        F.round("currency_sum_tier_value", 0).alias("currency_sum_tier_value_rounded"),
        F.ceil("tier_value_dense_rank_2").alias("tier_value_dense_rank_2_whole"),
    )
)

result = (
    valid
    .select(
        "id",
        "currency",
        "tier_value_dense_rank",
        "tier_value_rank",
        "tier_value_dense_rank_2",
        "currency_sum_tier_value_rounded",
    )
    .orderBy(F.col("tier_value_dense_rank_2").asc_nulls_last())
)
