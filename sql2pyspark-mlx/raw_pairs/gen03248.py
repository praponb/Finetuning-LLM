from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

scored = (
    lines
    .select(
        "currency",
        "tier_value",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
        F.rank().over(w3).alias("tier_value_rank"),
        F.rank().over(w4).alias("tier_value_rank_2"),
    )
)

cleaned = (
    scored
    .filter(F.col("tier_value_rank") <= 3)
    .select(
        "currency",
        "tier_value_cume_dist",
        "tier_value_percent_rank",
        "tier_value_rank",
        "tier_value_rank_2",
        F.when(F.col("tier_value_cume_dist") >= 0.68, "High").when(F.col("tier_value_cume_dist") >= 0.36, "Medium").otherwise("Low").alias("tier_value_cume_dist_band"),
    )
)

base = (
    cleaned
    .filter(F.col("tier_value_rank").between(3, 7))
    .select(
        "tier_value_cume_dist",
        "tier_value_percent_rank",
        "tier_value_rank",
        "tier_value_rank_2",
        F.when(F.col("currency").isin(["THB", "EUR"]), "Tier 1").otherwise("Other").alias("currency_group"),
    )
)

summary = (
    base
    .groupBy(
        F.when(F.col("tier_value_rank_2") >= 8, "PREMIUM").otherwise("STANDARD").alias("tier_value_rank_2_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("tier_value_percent_rank"), 2).alias("avg_tier_value_percent_rank"),
        F.count("*").alias("num_rows"),
        F.min("tier_value_rank").alias("min_tier_value_rank"),
        F.percentile_approx("tier_value_percent_rank", 0.5).alias("tier_value_percent_rank_p50"),
    )
    .filter(F.col("row_count") > 1)
)

result = (
    summary
    .select(
        "tier_value_rank_2_tier",
        "avg_tier_value_percent_rank",
        "min_tier_value_rank",
    )
)
