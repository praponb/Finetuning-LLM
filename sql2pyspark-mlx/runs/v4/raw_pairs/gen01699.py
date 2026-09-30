from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("line_total", "id")
w2 = Window.orderBy("line_total", "id")
w3 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("line_total", "id")

base = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .select("*", (F.col("quantity") - F.col("unit_price")).alias("line_total"))
)

result = (
    base
    .select(
        "*",
        F.ntile(10).over(w).alias("line_total_bucket"),
        F.lag("line_total", 1).over(w2).alias("prev_line_total"),
        F.first("line_total", ignorenulls=True).over(w3).alias("first_known_line_total"),
        F.ntile(10).over(w).alias("line_total_bucket_2"),
        F.row_number().over(w4).alias("event_seq"),
    )
)
