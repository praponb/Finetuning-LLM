from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy("lowest_price", "id").rowsBetween(-6, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "currency",
        "entry_price",
        "lowest_price",
        F.sum("lowest_price").over(w).alias("currency_sum_lowest_price"),
        F.avg("lowest_price").over(w2).alias("lowest_price_avg_6"),
    )
)
