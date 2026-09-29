from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan").orderBy(F.col("tier_value_int").desc_nulls_first())
w2 = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("tier_value"))
)

filtered = (
    lines
    .filter(F.col("tier_value") >= 72)
    .select(
        "*",
        F.when(F.col("plan").isin(["basic", "starter"]), "Core").otherwise("Other").alias("plan_group"),
        F.col("tier_value").cast("int").alias("tier_value_int"),
    )
)

with_windows = (
    filtered
    .filter(F.col("plan") == "enterprise")
    .select(
        "plan",
        "currency",
        "tier_value",
        "plan_group",
        F.percent_rank().over(w).alias("tier_value_int_percent_rank"),
        F.max("tier_value").over(w2).alias("plan_max_tier_value"),
    )
)

filtered_price_tiers = (
    with_windows
    .filter(F.col("tier_value") >= 39)
    .select(
        "currency",
        "tier_value",
        "plan_group",
        "plan_max_tier_value",
        (F.col("tier_value") / F.nullif("plan_max_tier_value", F.lit(0))).alias("tier_value_per_plan_max_tier_value"),
    )
)

valid = (
    filtered_price_tiers
    .filter(F.col("tier_value_per_plan_max_tier_value") > 79)
    .select(
        "*",
        F.round("tier_value_per_plan_max_tier_value", 0).alias("tier_value_per_plan_max_tier_value_rounded"),
        (F.col("plan_max_tier_value") * F.col("tier_value_per_plan_max_tier_value")).alias("plan_max_tier_value_times_tier_value_per_plan_max_tier_value"),
    )
)

result = (
    valid
    .select(
        "tier_value",
        "plan_group",
        "tier_value_per_plan_max_tier_value_rounded",
        "plan_max_tier_value_times_tier_value_per_plan_max_tier_value",
    )
)
