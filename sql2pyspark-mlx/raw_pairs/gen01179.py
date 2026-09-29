from pyspark.sql import functions as F

result = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)
