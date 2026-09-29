from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w_recent = Window.orderBy(F.col("tier_value").desc())
w3 = Window.orderBy(F.col("tier_value_dense_rank").asc_nulls_last())
w4 = Window.orderBy("tier_value_cume_dist")
w5 = Window.orderBy(F.col("tier_value_cume_dist").desc())
w6 = Window.orderBy(F.col("tier_value_dense_rank_cume_dist").desc())
w7 = Window.partitionBy("tier_value_cume_dist_percent_rank_tier").orderBy(F.col("tier_value_dense_rank_cume_dist_p90").desc())

lines = df_price_tiers.select("id", F.explode_outer("discounts").alias("tier_value"))

enriched = (
    lines
    .select(
        "id",
        "tier_value",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.rank().over(w_recent).alias("tier_value_rank"),
        F.cume_dist().over(w_recent).alias("tier_value_cume_dist_2"),
        F.dense_rank().over(w_recent).alias("tier_value_dense_rank"),
    )
)

metrics = (
    enriched
    .filter(F.col("tier_value_cume_dist_2") <= 0.7)
    .select(
        "tier_value_cume_dist",
        "tier_value_rank",
        F.percent_rank().over(w_recent).alias("tier_value_percent_rank"),
        F.cume_dist().over(w3).alias("tier_value_dense_rank_cume_dist"),
        F.rank().over(w4).alias("tier_value_cume_dist_rank"),
        F.percent_rank().over(w5).alias("tier_value_cume_dist_percent_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("tier_value_percent_rank").between(0.39, 0.41))
    .select(
        "*",
        (F.col("tier_value_rank") + F.col("tier_value_cume_dist_percent_rank")).alias("tier_value_rank_plus_tier_value_cume_dist_percent_rank"),
    )
)

metrics_price_tiers = (
    valid
    .select(
        "tier_value_cume_dist",
        "tier_value_dense_rank_cume_dist",
        "tier_value_cume_dist_percent_rank",
        "tier_value_rank_plus_tier_value_cume_dist_percent_rank",
        F.dense_rank().over(w6).alias("tier_value_dense_rank_cume_dist_dense_rank"),
    )
)

filtered = (
    metrics_price_tiers
    .filter(F.col("tier_value_cume_dist_percent_rank") >= 0.2)
    .select(
        "tier_value_dense_rank_cume_dist",
        "tier_value_cume_dist_percent_rank",
        "tier_value_rank_plus_tier_value_cume_dist_percent_rank",
        "tier_value_dense_rank_cume_dist_dense_rank",
        (F.col("tier_value_rank_plus_tier_value_cume_dist_percent_rank") + F.col("tier_value_cume_dist_percent_rank")).alias("tier_value_rank_plus_tier_value_cume_dist_percent_rank_plus_tier_value_cume_dist_percent_rank"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("tier_value_cume_dist_percent_rank") >= 0.83, "PREMIUM").otherwise("STANDARD").alias("tier_value_cume_dist_percent_rank_tier"),
        F.when(F.col("tier_value_dense_rank_cume_dist") >= 0.61, "HIGH").otherwise("LOW").alias("tier_value_dense_rank_cume_dist_tier"),
        F.when(F.col("tier_value_rank_plus_tier_value_cume_dist_percent_rank") >= 89, "HIGH").otherwise("LOW").alias("tier_value_rank_plus_tier_value_cume_dist_percent_rank_tier"),
    )
    .agg(
        F.round(F.avg("tier_value_rank_plus_tier_value_cume_dist_percent_rank"), 2).alias("avg_tier_value_rank_plus_tier_value_cume_dist_percent_rank"),
        F.percentile_approx("tier_value_dense_rank_cume_dist", 0.9).alias("tier_value_dense_rank_cume_dist_p90"),
        F.percentile_approx("tier_value_rank_plus_tier_value_cume_dist_percent_rank_plus_tier_value_cume_dist_percent_rank", 0.5).alias("tier_value_rank_plus_tier_value_cume_dist_percent_rank_plus_tier_value_cume_dist_percent_rank_p50"),
        F.round(F.avg("tier_value_dense_rank_cume_dist_dense_rank"), 2).alias("avg_tier_value_dense_rank_cume_dist_dense_rank"),
    )
    .withColumn("tier_value_dense_rank_cume_dist_p90_percent_rank", F.percent_rank().over(w7))
)
