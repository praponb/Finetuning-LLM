from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .groupBy("currency")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows_2"),
    )
    .orderBy(F.col("event_count").desc_nulls_first())
)
