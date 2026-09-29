from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("plan").isin(["enterprise", "basic", "pro"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .cube(
        F.date_trunc("month", "effective_from").alias("activity_month"),
        F.when(F.col("entry_price") >= 24, "LARGE").otherwise("SMALL").alias("entry_price_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_rows"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.avg(F.when(F.col("plan") == "pro", F.col("entry_price"))).alias("pro_avg_entry_price"),
        F.avg("entry_price").alias("avg_entry_price"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price_2"),
        F.count("*").alias("cnt"),
    )
)
