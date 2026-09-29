from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("lowest_price", "id")
w2 = Window.orderBy("effective_from", "id")
w3 = Window.orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("lowest_price").desc(), "id")

flattened_attrs = (
    df_price_tiers
    .filter(F.year("effective_from") >= 2025)
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    flattened_attrs
    .withColumn("prev_avg_tier_price", F.lag("avg_tier_price", 2).over(w))
    .withColumn("prev_lowest_price", F.lag("lowest_price", 2).over(w2))
    .withColumn("running_avg_tier_price", F.sum("avg_tier_price").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)
