from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("lowest_price", "id")
w2 = Window.partitionBy(F.month("effective_from")).orderBy(F.col("entry_price").desc(), "id")
w3 = Window.orderBy(F.col("lowest_price").desc(), "id")
w4 = Window.orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("effective_from")).orderBy("effective_from", "id")

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from") > "2024-03-01")
    .select(
        "id",
        "effective_from",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "effective_from",
        F.lag("entry_price", 1).over(w).alias("prev_entry_price"),
        F.ntile(5).over(w2).alias("entry_price_bucket"),
        F.ntile(4).over(w3).alias("lowest_price_bucket"),
        F.sum("entry_price").over(w4).alias("cumulative_entry_price"),
        F.lag("lowest_price", 2).over(w5).alias("prev_lowest_price"),
    )
)
