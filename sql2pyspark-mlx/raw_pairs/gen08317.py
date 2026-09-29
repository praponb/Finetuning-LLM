from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("order_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id")

cleaned = (
    df_order_items
    .filter(F.col("order_id").isin([1, 14, 39]))
    .select("*", (F.col("quantity") + F.col("unit_price")).alias("line_total"))
)

result = (
    cleaned
    .select(
        "id",
        "unit_price",
        "line_total",
        F.corr("unit_price", "quantity").over(w).alias("corr_unit_price_quantity"),
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)
