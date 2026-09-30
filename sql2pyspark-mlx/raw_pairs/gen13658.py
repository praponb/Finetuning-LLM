from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("avg_tier_price").desc_nulls_last())
w2 = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "plan",
        "avg_tier_price",
        F.dense_rank().over(w).alias("avg_tier_price_dense_rank"),
        F.sum("avg_tier_price").over(w2).alias("running_avg_tier_price"),
        F.first("avg_tier_price", ignorenulls=True).over(w3).alias("first_known_avg_tier_price"),
    )
)
