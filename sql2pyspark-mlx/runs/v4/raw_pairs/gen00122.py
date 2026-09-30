from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("lowest_price").desc(), "id")
w2 = Window.partitionBy("plan").orderBy("avg_tier_price")
w3 = Window.partitionBy("plan").orderBy("lowest_price", "id").rowsBetween(-5, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("avg_tier_price_cume_dist", F.cume_dist().over(w2))
    .withColumn("lowest_price_avg_5", F.avg("lowest_price").over(w3))
)
