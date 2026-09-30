from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("avg_tier_price", "id")
w_part = Window.partitionBy("currency").orderBy(F.col("entry_price").desc(), "id")

unpacked = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "lowest_price",
        "avg_tier_price",
        F.lag("avg_tier_price", 1).over(w).alias("prev_avg_tier_price"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)
