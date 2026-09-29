from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy(F.col("avg_tier_price").desc(), "id")

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .filter(F.col("avg_tier_price") < 25)
    .select(
        "*",
        F.sum("avg_tier_price").over(w).alias("running_avg_tier_price"),
        F.ntile(3).over(w2).alias("avg_tier_price_bucket"),
    )
)
