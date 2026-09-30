from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("lowest_price", "id")
w2 = Window.partitionBy("plan", "currency").orderBy("entry_price", "id")

unpacked = (
    df_price_tiers
    .filter(F.col("currency") != "EUR")
    .select(
        "id",
        "plan",
        "currency",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .filter(F.col("entry_price") <= 16)
    .select(
        "id",
        "plan",
        "currency",
        "lowest_price",
        F.lag("lowest_price", 1, 0).over(w).alias("prev_lowest_price"),
        F.lag("entry_price", 1).over(w2).alias("prev_entry_price"),
    )
)
