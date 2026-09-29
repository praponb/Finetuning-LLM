from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("MONTH", "effective_from").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("plan") == "pro", F.col("lowest_price")).otherwise(0)).alias("pro_lowest_price"),
        F.sum(F.when(F.col("plan") == "starter", F.col("avg_tier_price")).otherwise(0)).alias("starter_avg_tier_price"),
    )
    .filter(F.col("pro_lowest_price") < 0)
    .orderBy("pro_lowest_price")
)
