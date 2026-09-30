from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("lowest_price", "id")

unpacked = (
    df_price_tiers
    .select(
        "id",
        "tier_limits",
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "entry_price",
        F.lag("lowest_price").over(w).alias("prev_lowest_price"),
        F.lag("entry_price").over(w).alias("prev_entry_price"),
        F.row_number().over(w).alias("event_seq"),
    )
)
