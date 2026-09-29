from pyspark.sql import functions as F

result = (
    df_price_tiers
    .filter(F.col("effective_from") >= "2024-02-01")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)
