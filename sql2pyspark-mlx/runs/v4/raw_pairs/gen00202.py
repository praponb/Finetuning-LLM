from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("MONTH", "effective_from").alias("activity_month"))
    .agg(
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.percentile_approx("avg_tier_price", 0.5).alias("avg_tier_price_p50"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
    )
)
