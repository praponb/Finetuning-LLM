from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("avg_tier_price", "id")
w2 = Window.orderBy("avg_tier_price", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy(F.col("avg_tier_price").desc_nulls_last())

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency") == "EUR")
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("avg_tier_price") <= 27)
    .select(
        "id",
        "plan",
        "currency",
        "avg_tier_price",
        "entry_price",
        F.lead("lowest_price", 2, 0).over(w).alias("next_lowest_price"),
        F.avg("avg_tier_price").over(w2).alias("avg_tier_price_avg_5"),
        F.cume_dist().over(w3).alias("avg_tier_price_cume_dist"),
    )
)
