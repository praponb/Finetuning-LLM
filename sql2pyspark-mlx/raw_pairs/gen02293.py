from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())
w_recent = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("unit_price") <= 280)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.avg("unit_price").over(w_running).alias("unit_price_avg_3"),
        F.rank().over(w_grp).alias("unit_price_rank"),
        F.sum("quantity").over(w_recent).alias("cumulative_quantity_2"),
    )
)
