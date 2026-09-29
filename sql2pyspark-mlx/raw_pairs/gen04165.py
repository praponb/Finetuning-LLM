from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("effective_from"), "plan").orderBy(F.col("tier_value").desc())
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.orderBy("tier_value")
w4 = Window.partitionBy("plan").orderBy(F.col("tier_value_rank_2").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("tier_value_rank"),
        F.rank().over(w2).alias("tier_value_rank_2"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank"),
    )
)

cleaned = (
    metrics
    .filter((F.col("effective_from") > F.lit("2024-05-03").cast("date")) & (F.col("tier_value") <= 52))
    .withColumn("effective_from_minus_61d", F.date_sub("effective_from", 61))
    .withColumn("greatest_tier_value_tier_value_dense_rank", F.greatest("tier_value", "tier_value_dense_rank"))
)

metrics_price_tiers = (
    cleaned
    .filter(F.col("plan").isin(["starter", "pro", "enterprise"]))
    .select(
        "effective_from",
        "tier_value_rank",
        "tier_value_rank_2",
        "greatest_tier_value_tier_value_dense_rank",
        F.cume_dist().over(w4).alias("tier_value_rank_2_cume_dist"),
    )
)

valid = (
    metrics_price_tiers
    .filter(F.col("tier_value_rank_2_cume_dist").between(0.6, 0.79))
    .select("*", F.date_add("effective_from", 26).alias("effective_from_plus_26d"))
)

result = (
    valid
    .select(
        "effective_from",
        "tier_value_rank",
        "tier_value_rank_2",
        "greatest_tier_value_tier_value_dense_rank",
        "tier_value_rank_2_cume_dist",
        "effective_from_plus_26d",
    )
)
