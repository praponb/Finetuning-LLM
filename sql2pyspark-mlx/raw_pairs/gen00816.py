from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("effective_from", "id")
w2 = Window.partitionBy(F.year("effective_from"), "plan").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("currency", "plan").orderBy("avg_tier_price", "id")
w4 = Window.partitionBy(F.date_trunc("YEAR", "effective_from"), "currency").orderBy(F.col("avg_tier_price").desc(), "id")

unpacked = (
    df_price_tiers
    .filter(F.col("plan") == "starter")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.lead("avg_tier_price", 2, 0).over(w).alias("next_avg_tier_price"),
        F.sum("avg_tier_price").over(w2).alias("running_avg_tier_price"),
        F.lag("avg_tier_price", 1).over(w3).alias("prev_avg_tier_price"),
        F.ntile(10).over(w4).alias("avg_tier_price_bucket"),
    )
)
