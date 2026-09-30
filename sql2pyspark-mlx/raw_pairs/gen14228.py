from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("effective_from", "id")
w2 = Window.partitionBy("plan").orderBy(F.col("avg_tier_price").desc(), "id")

unpacked = (
    df_price_tiers
    .filter(F.col("plan") == "basic")
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "plan",
        F.lag("avg_tier_price").over(w).alias("prev_avg_tier_price"),
        F.lead("avg_tier_price").over(w).alias("next_avg_tier_price"),
        F.row_number().over(w).alias("event_seq"),
        F.ntile(3).over(w2).alias("avg_tier_price_bucket"),
    )
)
