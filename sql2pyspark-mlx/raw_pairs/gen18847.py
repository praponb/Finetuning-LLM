from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("lowest_price", "id")
w2 = Window.orderBy("effective_from", "id")
w3 = Window.partitionBy(F.date_trunc("month", "effective_from")).orderBy("entry_price", "id")
w4 = Window.orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .filter(F.col("effective_from") >= "2025-08-22")
    .select(
        "id",
        "effective_from",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "lowest_price",
        F.row_number().over(w).alias("seq_no"),
        F.lag("entry_price", 2).over(w2).alias("prev_entry_price"),
        F.lag("entry_price", 1).over(w3).alias("prev_entry_price_2"),
        F.row_number().over(w).alias("seq_no_2"),
        F.sum("lowest_price").over(w4).alias("cumulative_lowest_price"),
    )
)
