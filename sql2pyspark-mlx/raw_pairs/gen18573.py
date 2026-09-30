from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("avg_tier_price", "id")
w2 = Window.orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "effective_from",
        "avg_tier_price",
        F.lead("avg_tier_price", 1).over(w).alias("next_avg_tier_price"),
        F.sum("avg_tier_price").over(w2).alias("cumulative_avg_tier_price"),
        F.sum("avg_tier_price").over(w3).alias("cumulative_avg_tier_price_2"),
    )
)
