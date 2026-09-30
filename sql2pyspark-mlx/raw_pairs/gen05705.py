from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.year("effective_from") >= 2023)
    .select(
        "id",
        "effective_from",
        "currency",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("MONTH", "effective_from").alias("activity_month"))
    .agg(
        F.countDistinct("currency").alias("distinct_currencies"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .orderBy(F.col("distinct_currencies").desc())
)
