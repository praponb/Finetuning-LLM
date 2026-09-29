from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy(F.year("effective_from"), "plan")
w_grp = Window.partitionBy(F.date_trunc("week", "effective_from"))
w_part = Window.orderBy(F.col("tier_value").asc_nulls_first())
w4 = Window.orderBy(F.col("tier_value").desc())
w5 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w6 = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_first())
w_running = Window.partitionBy(F.month("effective_from")).orderBy(F.col("effective_from_period_avg_tier_value").asc_nulls_first())
w8 = Window.orderBy(F.col("effective_from_year_count_tier_value").asc_nulls_last())
w9 = Window.orderBy(F.col("effective_from_period_avg_tier_value_rank").desc_nulls_last())
w10 = Window.orderBy(F.col("tier_value_percent_rank").desc())
w11 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w12 = Window.orderBy(F.col("effective_from_year_count_tier_value_rank").desc_nulls_last())
w13 = Window.orderBy(F.col("tier_value_dense_rank").asc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

scored = (
    flattened
    .select(
        "effective_from",
        "tier_value",
        F.count("tier_value").over(w_recent).alias("effective_from_year_count_tier_value"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.avg("tier_value").over(w_grp).alias("effective_from_period_avg_tier_value"),
        F.dense_rank().over(w_part).alias("tier_value_dense_rank"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist"),
    )
)

with_windows = (
    metrics
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w5))
    .withColumn("tier_value_cume_dist_rank", F.rank().over(w6))
    .withColumn("tier_value_cume_dist_2", F.cume_dist().over(w4))
    .withColumn("effective_from_period_avg_tier_value_rank", F.rank().over(w_running))
)

scored_price_tiers = (
    with_windows
    .withColumn("effective_from_year_count_tier_value_rank", F.rank().over(w8))
    .withColumn("tier_value_percent_rank_2", F.percent_rank().over(w4))
    .withColumn("effective_from_period_avg_tier_value_rank_cume_dist", F.cume_dist().over(w9))
)

cleaned = (
    scored_price_tiers
    .filter(F.col("effective_from") > F.lit("2024-04-29").cast("date"))
    .select(
        "*",
        F.round((F.col("tier_value_cume_dist_2") / F.col("tier_value_cume_dist_rank")), 2).alias("tier_value_cume_dist_2_per_tier_value_cume_dist_rank"),
    )
)

with_windows_price_tiers = (
    cleaned
    .select(
        "effective_from",
        "tier_value",
        "effective_from_year_count_tier_value",
        "tier_value_dense_rank",
        "tier_value_cume_dist",
        "tier_value_percent_rank",
        "tier_value_cume_dist_rank",
        "tier_value_cume_dist_2",
        "effective_from_period_avg_tier_value_rank",
        "effective_from_year_count_tier_value_rank",
        "tier_value_percent_rank_2",
        "tier_value_cume_dist_2_per_tier_value_cume_dist_rank",
        F.rank().over(w10).alias("tier_value_percent_rank_rank"),
        F.rank().over(w11).alias("tier_value_rank"),
        F.cume_dist().over(w6).alias("tier_value_cume_dist_cume_dist"),
        F.rank().over(w12).alias("effective_from_year_count_tier_value_rank_rank"),
        F.rank().over(w13).alias("tier_value_dense_rank_rank"),
    )
)

result = (
    with_windows_price_tiers
    .select(
        "effective_from",
        "effective_from_year_count_tier_value",
        "tier_value_dense_rank",
        "tier_value_cume_dist",
        "tier_value_cume_dist_rank",
        "effective_from_year_count_tier_value_rank",
        "tier_value_cume_dist_2_per_tier_value_cume_dist_rank",
        "tier_value_percent_rank_rank",
        "tier_value_rank",
    )
)
