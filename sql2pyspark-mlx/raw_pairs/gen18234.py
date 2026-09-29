from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.orderBy(F.col("plan_avg_plan_avg_tier_value").desc_nulls_last())
w_part = Window.orderBy(F.col("plan_max_tier_value").asc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

with_windows = (
    lines
    .filter(F.col("plan") == "pro")
    .select(
        "id",
        "plan",
        "tier_value",
        F.avg("tier_value").over(w).alias("plan_avg_tier_value"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.avg("plan_avg_tier_value").over(w).alias("plan_avg_plan_avg_tier_value"),
        F.max("tier_value").over(w).alias("plan_max_tier_value"),
    )
)

cleaned = (
    enriched
    .filter(F.col("plan_max_tier_value") == 27)
    .select(
        "tier_value",
        "plan_avg_tier_value",
        "plan_avg_plan_avg_tier_value",
        "plan_max_tier_value",
        F.least("tier_value", "plan_avg_tier_value").alias("least_tier_value_plan_avg_tier_value"),
    )
)

with_windows_price_tiers = (
    cleaned
    .select(
        "tier_value",
        "plan_avg_plan_avg_tier_value",
        "plan_max_tier_value",
        "least_tier_value_plan_avg_tier_value",
        F.rank().over(w2).alias("tier_value_rank"),
    )
)

metrics = (
    with_windows_price_tiers
    .select(
        "tier_value",
        "tier_value_rank",
        F.dense_rank().over(w3).alias("plan_avg_plan_avg_tier_value_dense_rank"),
        F.percent_rank().over(w_part).alias("plan_max_tier_value_percent_rank"),
    )
)

cleaned_price_tiers = (
    metrics
    .filter(F.col("plan_max_tier_value_percent_rank").between(0.25, 0.47))
    .select(
        "*",
        F.greatest("plan_avg_plan_avg_tier_value_dense_rank", "tier_value_rank").alias("greatest_plan_avg_plan_avg_tier_value_dense_rank_tier_value_rank"),
    )
)

result = (
    cleaned_price_tiers
    .select(
        "tier_value",
        "plan_avg_plan_avg_tier_value_dense_rank",
        "plan_max_tier_value_percent_rank",
    )
    .orderBy("plan_max_tier_value_percent_rank", F.col("tier_value").desc())
)
