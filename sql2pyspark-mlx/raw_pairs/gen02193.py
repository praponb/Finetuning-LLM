from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("entry_price", "id")
w2 = Window.partitionBy("plan").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "currency",
        "lowest_price",
        "entry_price",
        F.row_number().over(w).alias("event_seq"),
        F.sum("lowest_price").over(w2).alias("cumulative_lowest_price"),
    )
)
