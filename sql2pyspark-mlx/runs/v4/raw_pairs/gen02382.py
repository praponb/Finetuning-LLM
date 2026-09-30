from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.orderBy("tier_value")
w4 = Window.orderBy(F.col("tier_value_cume_dist").desc())
w5 = Window.orderBy(F.col("tier_value").desc())
w6 = Window.orderBy(F.col("tier_value_rank").desc_nulls_last())
w7 = Window.orderBy(F.col("tier_value_cume_dist_cume_dist").desc_nulls_first())
w8 = Window.orderBy(F.col("tier_value_rank_dense_rank").asc_nulls_last())
w9 = Window.orderBy(F.col("tier_value_cume_dist_2").desc_nulls_first())

flattened = df_price_tiers.select("id", F.explode("discounts").alias("tier_value"))

scored = (
    flattened
    .withColumn("tier_value_rank", F.rank().over(w_recent))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
    .withColumn("tier_value_cume_dist_2", F.cume_dist().over(w3))
)

with_windows = (
    scored
    .withColumn("tier_value_cume_dist_cume_dist", F.cume_dist().over(w4))
    .withColumn("tier_value_cume_dist_3", F.cume_dist().over(w5))
    .withColumn("tier_value_rank_dense_rank", F.dense_rank().over(w6))
)

enriched = (
    with_windows
    .withColumn("tier_value_cume_dist_cume_dist_2", F.cume_dist().over(w4))
    .withColumn("tier_value_cume_dist_cume_dist_dense_rank", F.dense_rank().over(w7))
)

with_windows_price_tiers = (
    enriched
    .withColumn("tier_value_rank_percent_rank", F.percent_rank().over(w6))
    .withColumn("tier_value_rank_dense_rank_rank", F.rank().over(w8))
    .withColumn("tier_value_cume_dist_2_cume_dist", F.cume_dist().over(w9))
)

filtered = (
    with_windows_price_tiers
    .filter(F.col("tier_value_cume_dist_cume_dist_dense_rank") <= 4)
    .select(
        "id",
        "tier_value",
        "tier_value_rank",
        "tier_value_cume_dist",
        "tier_value_cume_dist_2",
        "tier_value_cume_dist_3",
        "tier_value_rank_dense_rank",
        "tier_value_cume_dist_cume_dist_2",
        "tier_value_rank_percent_rank",
        "tier_value_rank_dense_rank_rank",
        (F.col("tier_value_cume_dist_2") - F.col("tier_value_cume_dist_cume_dist_dense_rank")).alias("tier_value_cume_dist_2_minus_tier_value_cume_dist_cume_dist_dense_rank"),
    )
)

valid = (
    filtered
    .filter(F.col("tier_value_cume_dist_2").between(0.23, 0.49))
    .select(
        "id",
        "tier_value",
        "tier_value_rank",
        "tier_value_cume_dist",
        "tier_value_cume_dist_2",
        "tier_value_cume_dist_3",
        "tier_value_rank_dense_rank",
        "tier_value_cume_dist_cume_dist_2",
        F.coalesce("tier_value_cume_dist", F.lit(0)).alias("tier_value_cume_dist_filled"),
        (F.col("tier_value_cume_dist_2_minus_tier_value_cume_dist_cume_dist_dense_rank") / F.nullif("tier_value_cume_dist_cume_dist_2", F.lit(0))).alias("tier_value_cume_dist_2_minus_tier_value_cume_dist_cume_dist_dense_rank_per_tier_value_cume_dist_cume_dist_2"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("tier_value_cume_dist_filled") >= 81, "PREMIUM").otherwise("STANDARD").alias("tier_value_cume_dist_filled_tier"),
        F.when(F.col("tier_value_rank_dense_rank") >= 6, "HIGH").otherwise("LOW").alias("tier_value_rank_dense_rank_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("tier_value_cume_dist_filled"), 2).alias("avg_tier_value_cume_dist_filled"),
        F.round(F.avg("tier_value_cume_dist_2"), 2).alias("avg_tier_value_cume_dist_2"),
    )
    .orderBy("tier_value_rank_dense_rank_tier")
)
