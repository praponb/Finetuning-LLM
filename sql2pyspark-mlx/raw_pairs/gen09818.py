from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy(F.month("effective_from"), "currency").orderBy("entry_price", "id")

parsed = (
    df_price_tiers
    .filter(F.year("effective_from") >= 2025)
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .filter(F.col("effective_from") >= "2025-06-01")
    .select(
        "id",
        "effective_from",
        "avg_tier_price",
        F.avg("lowest_price").over(w).alias("lowest_price_avg_6_prior"),
        F.avg("avg_tier_price").over(w2).alias("avg_tier_price_avg_6"),
        F.ntile(3).over(w3).alias("entry_price_bucket"),
    )
)
