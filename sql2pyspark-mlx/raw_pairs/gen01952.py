from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_prices",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .groupBy("currency")
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.size(F.collect_set("currency")).alias("num_currencies_2"),
    )
    .orderBy(F.col("num_currencies_2").desc())
)
