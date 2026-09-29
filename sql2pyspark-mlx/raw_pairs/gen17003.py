from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "effective_from"), "plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w3 = Window.orderBy("effective_from_period_count_tier_value_plus_tier_value")
w4 = Window.orderBy(F.col("effective_from_period_count_tier_value_plus_tier_value_cume_dist").desc_nulls_last())
w5 = Window.partitionBy("plan").orderBy(F.col("effective_from_period_count_tier_value_plus_tier_value_cume_dist").asc_nulls_first())
w_recent = Window.partitionBy("plan")
w7 = Window.partitionBy(F.date_trunc("month", "effective_from_plus_months"), "plan")

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("tier_value"))
)

with_windows = (
    lines
    .filter(F.month("effective_from").isin([2, 5, 12]))
    .withColumn("effective_from_period_count_tier_value", F.count("tier_value").over(w))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
)

filtered = (
    with_windows
    .filter(F.col("plan") == "basic")
    .select(
        "plan",
        "effective_from",
        "effective_from_period_count_tier_value",
        "tier_value_cume_dist",
        F.when(F.col("tier_value_cume_dist") >= 0.71, "High").when(F.col("tier_value_cume_dist") >= 0.33, "Medium").otherwise("Low").alias("tier_value_cume_dist_level"),
        (F.col("effective_from_period_count_tier_value") + F.col("tier_value")).alias("effective_from_period_count_tier_value_plus_tier_value"),
    )
)

metrics = (
    filtered
    .select(
        "plan",
        "effective_from",
        "effective_from_period_count_tier_value",
        "tier_value_cume_dist_level",
        "effective_from_period_count_tier_value_plus_tier_value",
        F.cume_dist().over(w3).alias("effective_from_period_count_tier_value_plus_tier_value_cume_dist"),
    )
)

cleaned = (
    metrics
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-06-07")))
    .select(
        "plan",
        "effective_from",
        "tier_value_cume_dist_level",
        "effective_from_period_count_tier_value_plus_tier_value",
        "effective_from_period_count_tier_value_plus_tier_value_cume_dist",
        F.upper("plan").alias("plan_upper"),
        F.add_months("effective_from", 12).alias("effective_from_plus_months"),
    )
)

scored = (
    cleaned
    .withColumn("effective_from_period_count_tier_value_plus_tier_value_cume_dist_percent_rank", F.percent_rank().over(w4))
    .withColumn("effective_from_period_count_tier_value_plus_tier_value_cume_dist_percent_rank_2", F.percent_rank().over(w5))
    .withColumn("plan_min_effective_from_period_count_tier_value_plus_tier_value", F.min("effective_from_period_count_tier_value_plus_tier_value").over(w_recent))
)

result = (
    scored
    .select(
        "plan",
        "effective_from",
        "effective_from_period_count_tier_value_plus_tier_value",
        "plan_upper",
        "plan_min_effective_from_period_count_tier_value_plus_tier_value",
        F.avg("effective_from_period_count_tier_value_plus_tier_value_cume_dist").over(w7).alias("effective_from_plus_months_period_avg_effective_from_period_count_tier_value_plus_tier_value_cume_dist"),
        F.avg("effective_from_period_count_tier_value_plus_tier_value_cume_dist_percent_rank").over(w_recent).alias("plan_avg_effective_from_period_count_tier_value_plus_tier_value_cume_dist_percent_rank"),
    )
)
