from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy(F.month("effective_from"), "plan").orderBy(F.col("years_since_effective_from").asc_nulls_last())
w4 = Window.partitionBy(F.year("effective_from"), "plan").orderBy(F.col("tier_value").desc())
w5 = Window.partitionBy("plan").orderBy(F.col("plan_count_tier_value_whole").desc_nulls_first())
w6 = Window.partitionBy(F.year("effective_from")).orderBy("plan_count_tier_value_whole_cume_dist")
w7 = Window.orderBy(F.col("plan_count_tier_value_whole_cume_dist").desc_nulls_first())
w8 = Window.orderBy(F.col("years_since_effective_from_dense_rank").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("tier_value") >= 33)
    .withColumn("years_since_effective_from", (F.datediff(F.current_date(), "effective_from") / 365))
    .withColumn("tier_value_whole", F.ceil("tier_value"))
)

scored = (
    filtered
    .filter(F.col("effective_from") >= "2023-10-11")
    .select(
        "*",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.count("tier_value_whole").over(w2).alias("plan_count_tier_value_whole"),
    )
)

scored_price_tiers = (
    scored
    .select(
        "id",
        "effective_from",
        "tier_value",
        "years_since_effective_from",
        "tier_value_whole",
        F.dense_rank().over(w3).alias("years_since_effective_from_dense_rank"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist"),
        F.cume_dist().over(w5).alias("plan_count_tier_value_whole_cume_dist"),
    )
)

with_windows = (
    scored_price_tiers
    .select(
        "*",
        F.dense_rank().over(w6).alias("plan_count_tier_value_whole_cume_dist_dense_rank"),
        F.cume_dist().over(w7).alias("plan_count_tier_value_whole_cume_dist_cume_dist"),
        F.percent_rank().over(w8).alias("years_since_effective_from_dense_rank_percent_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("tier_value_cume_dist").between(0.31, 0.45))
    .select(
        "id",
        "effective_from",
        "tier_value",
        "tier_value_whole",
        "tier_value_cume_dist",
        "plan_count_tier_value_whole_cume_dist",
        "plan_count_tier_value_whole_cume_dist_dense_rank",
        "plan_count_tier_value_whole_cume_dist_cume_dist",
        "years_since_effective_from_dense_rank_percent_rank",
        F.floor("years_since_effective_from_dense_rank").alias("years_since_effective_from_dense_rank_whole"),
    )
)

result = (
    valid
    .withColumn("plan_count_tier_value_whole_cume_dist_cume_dist_tier", F.when(F.col("plan_count_tier_value_whole_cume_dist_cume_dist") >= 0.6, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "effective_from"))
    .cube("plan_count_tier_value_whole_cume_dist_cume_dist_tier", "quarter_start")
    .agg(
        F.avg("tier_value").alias("avg_tier_value"),
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("plan_count_tier_value_whole_cume_dist_dense_rank", [0.25, 0.5, 0.75]).alias("plan_count_tier_value_whole_cume_dist_dense_rank_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("plan_count_tier_value_whole_cume_dist_cume_dist_tier").desc())
)
