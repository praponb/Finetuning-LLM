from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_prices",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("entry_price") >= 26, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
        F.date_trunc("week", "effective_from").alias("week_start"),
    )
    .agg(
        F.max("entry_price").alias("max_entry_price"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
    )
    .filter(F.col("avg_entry_price") < 1000000)
    .orderBy("max_entry_price")
)
