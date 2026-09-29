from pyspark.sql import functions as F

result = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)
