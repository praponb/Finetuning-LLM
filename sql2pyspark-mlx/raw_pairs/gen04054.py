from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("adjusted_tier_value").desc_nulls_first())
w3 = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("discounts").alias("tier_value"))
)

cleaned = (
    lines
    .filter(F.col("tier_value") <= 72)
    .select(
        "*",
        F.round((F.col("tier_value") * 0.85), 2).alias("adjusted_tier_value"),
        F.trim("plan").alias("clean_plan"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.percent_rank().over(w2).alias("adjusted_tier_value_percent_rank"),
        F.sum("adjusted_tier_value").over(w3).alias("plan_sum_adjusted_tier_value"),
    )
)

cleaned_price_tiers = (
    scored
    .filter(F.col("tier_value_dense_rank").between(6, 8))
    .select(
        "id",
        "tier_value",
        "adjusted_tier_value",
        "clean_plan",
        "adjusted_tier_value_percent_rank",
        "plan_sum_adjusted_tier_value",
        F.round((F.col("adjusted_tier_value_percent_rank") * 100), 2).alias("adjusted_adjusted_tier_value_percent_rank"),
    )
)

result = (
    cleaned_price_tiers
    .select(
        "id",
        "tier_value",
        "adjusted_tier_value",
        "adjusted_tier_value_percent_rank",
    )
    .orderBy("adjusted_tier_value")
)
