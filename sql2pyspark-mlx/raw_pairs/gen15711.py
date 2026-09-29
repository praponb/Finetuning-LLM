from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .groupBy("currency")
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
    )
    .orderBy(F.col("eur_events").desc_nulls_last())
)
