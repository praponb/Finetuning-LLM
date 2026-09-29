from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(~F.col("plan").isin(["starter", "pro"]))
    .select(
        "id",
        "plan",
        "tier_prices",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("avg_tier_price") >= 27, "PREMIUM").otherwise("STANDARD").alias("avg_tier_price_tier"),
        "plan",
    )
    .agg(
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.sum(F.when(F.col("plan") == "starter", F.col("avg_tier_price")).otherwise(0)).alias("starter_avg_tier_price"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.count("*").alias("row_count"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("row_count") < 1000)
)
