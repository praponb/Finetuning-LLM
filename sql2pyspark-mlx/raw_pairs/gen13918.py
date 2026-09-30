from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.month("effective_from").isin([7, 11, 12]))
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("week", "effective_from").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sort_array(F.collect_set("plan")).alias("plan_list_2"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
    )
    .orderBy(F.col("enterprise_events").desc())
)
