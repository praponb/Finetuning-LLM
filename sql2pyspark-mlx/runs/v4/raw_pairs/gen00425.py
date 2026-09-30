from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count("*").alias("cnt"),
    )
)
