from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())
w4 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("quantity")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

result = (
    valid
    .filter(F.col("line_total").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "line_total",
        F.first("line_total", ignorenulls=True).over(w).alias("first_known_line_total"),
        F.lag("quantity", 1).over(w2).alias("prev_quantity"),
        F.dense_rank().over(w3).alias("unit_price_dense_rank"),
        F.sum("unit_price").over(w4).alias("running_unit_price"),
        F.dense_rank().over(w5).alias("quantity_dense_rank"),
    )
)
