from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").desc(), "id")
w2 = Window.orderBy(F.col("unit_price").desc_nulls_last())
w3 = Window.orderBy("unit_price_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("unit_price_rounded", "id")
w_grp = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("first_known_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("unit_price_rounded_bucket", "id")
w8 = Window.orderBy("running_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("unit_price_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "quantity",
        "unit_price",
        F.round("unit_price", 2).alias("unit_price_rounded"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.ntile(10).over(w).alias("unit_price_bucket"),
        F.percent_rank().over(w2).alias("unit_price_percent_rank"),
        F.first("unit_price", ignorenulls=True).over(w3).alias("first_known_unit_price"),
        F.ntile(5).over(w4).alias("unit_price_rounded_bucket"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("unit_price_bucket") <= 6)
    .select(
        "id",
        "quantity",
        "unit_price",
        "unit_price_rounded",
        "unit_price_percent_rank",
        "first_known_unit_price",
        "unit_price_rounded_bucket",
        F.round((F.col("unit_price_rounded_bucket") * 0.9), 2).alias("unit_price_rounded_bucket_scaled"),
    )
)

enriched = (
    cleaned
    .withColumn("running_quantity", F.sum("quantity").over(w_grp))
    .withColumn("cumulative_unit_price", F.sum("unit_price").over(w6))
)

result = (
    enriched
    .select(
        "id",
        "quantity",
        "unit_price_rounded",
        "unit_price_percent_rank",
        "unit_price_rounded_bucket",
        "running_quantity",
        "cumulative_unit_price",
        F.row_number().over(w_hist).alias("event_seq"),
        F.sum("cumulative_unit_price").over(w8).alias("cumulative_cumulative_unit_price"),
        F.last("unit_price_percent_rank", ignorenulls=True).over(w9).alias("last_known_unit_price_percent_rank"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.last("running_quantity", ignorenulls=True).over(w_grp).alias("last_known_running_quantity"),
    )
)
