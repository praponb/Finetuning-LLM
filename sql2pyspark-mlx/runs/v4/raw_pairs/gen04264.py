from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.orderBy("lowest_price", "id")
w3 = Window.orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.sum("entry_price").over(w_running).alias("cumulative_entry_price"),
        F.lag("lowest_price", 1).over(w_running_2).alias("prev_lowest_price"),
        F.sum("lowest_price").over(w3).alias("cumulative_lowest_price"),
        F.sum("entry_price").over(w_running).alias("cumulative_entry_price_2"),
        F.lag("entry_price", 1, 0).over(w_running_2).alias("prev_entry_price"),
    )
)
