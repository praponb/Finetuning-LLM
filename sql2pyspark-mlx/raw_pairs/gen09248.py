from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy("avg_tier_price", "id")

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "effective_from",
        "currency",
        "entry_price",
        F.min("avg_tier_price").over(w).alias("last3_min_avg_tier_price"),
        F.sum("avg_tier_price").over(w).alias("last3_sum_avg_tier_price"),
        F.max("avg_tier_price").over(w).alias("last3_max_avg_tier_price"),
        F.count("avg_tier_price").over(w).alias("last3_count_avg_tier_price"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
