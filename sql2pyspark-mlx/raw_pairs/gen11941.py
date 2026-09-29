from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("effective_from", "id").rowsBetween(-6, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "lowest_price",
        "avg_tier_price",
        F.sum("avg_tier_price").over(w_ordered).alias("last7_sum_avg_tier_price"),
        F.min("avg_tier_price").over(w_ordered).alias("last7_min_avg_tier_price"),
        F.max("avg_tier_price").over(w_ordered).alias("last7_max_avg_tier_price"),
        F.count("avg_tier_price").over(w_ordered).alias("last7_count_avg_tier_price"),
    )
)
