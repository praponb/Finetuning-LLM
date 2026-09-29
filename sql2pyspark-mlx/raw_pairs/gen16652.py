from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("entry_price", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("entry_price", "id")
w3 = Window.orderBy("entry_price", "id")

unpacked = (
    df_price_tiers
    .filter(F.col("currency").isin(["USD", "EUR"]))
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .withColumn("entry_price_avg_3", F.avg("entry_price").over(w))
    .withColumn("next_avg_tier_price", F.lead("avg_tier_price", 1, 0).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)
