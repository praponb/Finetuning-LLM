from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("lowest_price", "id").rowsBetween(-6, Window.currentRow)
w_part = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from").between("2023-03-01", "2024-01-10"))
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "plan",
        "lowest_price",
        F.avg("lowest_price").over(w).alias("lowest_price_avg_6"),
        F.sum("lowest_price").over(w_part).alias("cumulative_lowest_price"),
    )
)
