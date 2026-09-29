from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("currency").orderBy("avg_tier_price", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy(F.col("avg_tier_price").desc_nulls_first())

unpacked = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .filter(F.col("avg_tier_price") <= 20)
    .withColumn("last4_avg_avg_tier_price", F.avg("avg_tier_price").over(w_grp))
    .withColumn("last4_sum_avg_tier_price", F.sum("avg_tier_price").over(w_grp))
    .withColumn("last4_count_avg_tier_price", F.count("avg_tier_price").over(w_grp))
    .withColumn("last4_min_avg_tier_price", F.min("avg_tier_price").over(w_grp))
    .withColumn("avg_tier_price_dense_rank", F.dense_rank().over(w2))
)
