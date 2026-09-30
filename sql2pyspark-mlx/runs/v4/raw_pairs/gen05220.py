from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("avg_tier_price") >= 30, "PREMIUM").otherwise("STANDARD").alias("avg_tier_price_tier"),
        "currency",
    )
    .agg(
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("avg_tier_price")).otherwise(0)).alias("eur_avg_tier_price"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count_2"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count_3"),
    )
)
