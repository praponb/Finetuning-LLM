from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan").orderBy("effective_from", "id")
w2 = Window.partitionBy("plan").orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_price_tiers
    .filter(F.month("effective_from").isin([1, 4, 8]))
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.lag("entry_price", 2).over(w_hist).alias("prev_entry_price"),
        F.sum("avg_tier_price").over(w2).alias("cumulative_avg_tier_price"),
    )
)
