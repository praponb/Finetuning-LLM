from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("entry_price").desc(), "id")
w2 = Window.partitionBy("currency").orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("lowest_price", "id")

unpacked = (
    df_price_tiers
    .filter(F.col("currency").isin(["EUR", "USD", "THB"]))
    .select(
        "id",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "currency",
        "avg_tier_price",
        F.ntile(5).over(w).alias("entry_price_bucket"),
        F.sum("entry_price").over(w2).alias("running_entry_price"),
        F.lag("avg_tier_price", 2).over(w_ordered).alias("prev_avg_tier_price"),
    )
)
