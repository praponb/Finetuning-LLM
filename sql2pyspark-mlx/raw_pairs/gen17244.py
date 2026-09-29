from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("line_total", "id")
w3 = Window.orderBy(F.col("quantity").asc_nulls_last())

cleaned = (
    df_order_items
    .filter(F.col("product_id").isin([15, 20, 35]))
    .withColumn("line_total", (F.col("unit_price") * F.col("quantity")))
)

result = (
    cleaned
    .select(
        "id",
        "product_id",
        "unit_price",
        "line_total",
        F.first("line_total", ignorenulls=True).over(w).alias("first_known_line_total"),
        F.ntile(10).over(w2).alias("line_total_bucket"),
        F.rank().over(w3).alias("quantity_rank"),
    )
)
