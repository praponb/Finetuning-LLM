from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("plan") != "enterprise")
    .withColumn("days_since_effective_from", F.datediff(F.current_date(), "effective_from"))
)

with_windows = (
    cleaned
    .filter(F.col("tier_value").isNotNull())
    .withColumn("plan_min_days_since_effective_from", F.min("days_since_effective_from").over(w))
)

filtered = (
    with_windows
    .filter((F.col("effective_from") > F.lit("2024-06-06").cast("date")) & (F.col("plan_min_days_since_effective_from") > 38))
    .select(
        "id",
        "tier_value",
        "days_since_effective_from",
        "plan_min_days_since_effective_from",
        F.coalesce("tier_value", F.lit(0)).alias("tier_value_filled"),
        F.coalesce("tier_value", F.lit(0)).alias("tier_value_filled_2"),
    )
)

result = (
    filtered
    .select(
        "id",
        "tier_value",
        "plan_min_days_since_effective_from",
        "tier_value_filled",
        "tier_value_filled_2",
    )
    .orderBy(
        F.col("plan_min_days_since_effective_from").desc(),
        F.col("tier_value").desc(),
    )
)
