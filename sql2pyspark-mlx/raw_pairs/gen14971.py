from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency").orderBy("lowest_price", "id")
w2 = Window.partitionBy(F.month("effective_from"), "currency")
w3 = Window.partitionBy(F.month("effective_from"), "currency").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency").isin(["EUR", "THB", "USD"]))
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "effective_from",
        "currency",
        "lowest_price",
        F.row_number().over(w_recent).alias("event_seq"),
        F.lag("lowest_price", 1).over(w_recent).alias("prev_lowest_price"),
        F.sum("lowest_price").over(w2).alias("effective_from_month_sum_lowest_price"),
        F.first("lowest_price", ignorenulls=True).over(w3).alias("first_known_lowest_price"),
    )
)
