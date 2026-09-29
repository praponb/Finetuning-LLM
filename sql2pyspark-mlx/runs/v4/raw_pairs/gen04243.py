from pyspark.sql import functions as F

result = (
    df_price_tiers
    .filter(F.col("plan") == "basic")
    .select(
        "id",
        "plan",
        "tier_limits",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)
