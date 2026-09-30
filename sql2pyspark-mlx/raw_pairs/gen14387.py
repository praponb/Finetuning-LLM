from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("lowest_price", "id").rowsBetween(-2, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "tier_limits",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "avg_tier_price",
        "lowest_price",
        F.max("avg_tier_price").over(w).alias("last3_max_avg_tier_price"),
        F.min("avg_tier_price").over(w).alias("last3_min_avg_tier_price"),
        F.sum("avg_tier_price").over(w).alias("last3_sum_avg_tier_price"),
        F.avg("avg_tier_price").over(w).alias("last3_avg_avg_tier_price"),
    )
)
