from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("week", "effective_from").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
    )
    .orderBy(F.col("usd_rows").desc_nulls_first())
)
