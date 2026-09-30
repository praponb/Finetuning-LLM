from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy(F.col("avg_tier_price").asc_nulls_last())

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency").isin(["EUR", "USD", "THB"]))
    .select(
        "id",
        "currency",
        "tier_limits",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("lowest_price") <= 14)
    .withColumn("cumulative_entry_price", F.sum("entry_price").over(w))
    .withColumn("avg_tier_price_dense_rank", F.dense_rank().over(w2))
)
