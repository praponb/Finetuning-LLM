from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("avg_tier_price", "id")
w_hist = Window.partitionBy("currency", "plan").orderBy("effective_from", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy(F.col("lowest_price").desc(), "id")
w4 = Window.partitionBy("currency").orderBy(F.col("avg_tier_price").asc_nulls_first())
w5 = Window.orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .filter(F.col("lowest_price") <= 18)
    .select(
        "id",
        "plan",
        "effective_from",
        "entry_price",
        F.row_number().over(w).alias("event_seq"),
        F.avg("lowest_price").over(w_hist).alias("lowest_price_avg_6"),
        F.ntile(3).over(w3).alias("lowest_price_bucket"),
        F.rank().over(w4).alias("avg_tier_price_rank"),
        F.sum("entry_price").over(w5).alias("cumulative_entry_price"),
    )
)
