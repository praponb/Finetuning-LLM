from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("entry_price", "id").rowsBetween(-2, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan") == "starter")
    .select(
        "id",
        "plan",
        "tier_prices",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "lowest_price",
        "avg_tier_price",
        F.count("avg_tier_price").over(w).alias("last3_count_avg_tier_price"),
        F.max("avg_tier_price").over(w).alias("last3_max_avg_tier_price"),
        F.sum("avg_tier_price").over(w).alias("last3_sum_avg_tier_price"),
    )
)
