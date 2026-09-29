from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("entry_price") >= 33, "HIGH").otherwise("LOW").alias("entry_price_tier"),
        "currency",
    )
    .agg(
        F.sum(F.when(F.col("currency") == "THB", F.col("entry_price")).otherwise(0)).alias("thb_entry_price"),
        F.avg(F.when(F.col("currency") == "EUR", F.col("avg_tier_price"))).alias("eur_avg_avg_tier_price"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .orderBy(F.col("entry_price_tier").desc_nulls_last())
)
