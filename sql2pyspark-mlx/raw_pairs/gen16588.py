from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("effective_from").between("2023-03-17", "2025-10-01"))
    .select(
        "id",
        "effective_from",
        "tier_limits",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .groupBy(
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("distinct_ids_2").desc())
)
