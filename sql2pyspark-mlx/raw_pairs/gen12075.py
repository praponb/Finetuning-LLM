from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("entry_price").desc(), "id")
w2 = Window.partitionBy("plan").orderBy("avg_tier_price")
w3 = Window.partitionBy("plan").orderBy(F.col("avg_tier_price").desc(), "id")

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .filter(F.col("entry_price") > 14)
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.percent_rank().over(w2).alias("avg_tier_price_percent_rank"),
        F.ntile(5).over(w3).alias("avg_tier_price_bucket"),
    )
)
