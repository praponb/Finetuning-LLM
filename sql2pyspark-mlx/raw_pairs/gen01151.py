from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("plan").orderBy(F.col("tier_value_percent_rank").asc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("plan_min_tier_value").desc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

metrics = (
    flattened
    .filter(F.col("plan") != "basic")
    .select(
        "id",
        "plan",
        "tier_value",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.stddev_samp("tier_value").over(w2).alias("currency_stddev_samp_tier_value"),
        F.min("tier_value").over(w3).alias("plan_min_tier_value"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "tier_value_percent_rank",
        "plan_min_tier_value",
        F.rank().over(w4).alias("tier_value_percent_rank_rank"),
        F.percent_rank().over(w5).alias("plan_min_tier_value_percent_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("tier_value_percent_rank") >= 0.79)
    .select(
        "id",
        "tier_value_percent_rank",
        "plan_min_tier_value",
        "tier_value_percent_rank_rank",
        "plan_min_tier_value_percent_rank",
        F.round((F.col("tier_value_percent_rank") / F.col("tier_value_percent_rank_rank")), 2).alias("tier_value_percent_rank_per_tier_value_percent_rank_rank"),
        F.coalesce("tier_value_percent_rank", F.lit(0)).alias("tier_value_percent_rank_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("tier_value_percent_rank") >= 0.7, "HIGH").otherwise("LOW").alias("tier_value_percent_rank_tier"),
    )
    .agg(
        F.max("tier_value_percent_rank_rank").alias("max_tier_value_percent_rank_rank"),
        F.avg("tier_value_percent_rank_filled").alias("avg_tier_value_percent_rank_filled"),
        F.percentile_approx("tier_value_percent_rank", 0.95).alias("tier_value_percent_rank_p95"),
    )
    .orderBy(F.col("tier_value_percent_rank_p95").desc())
)
