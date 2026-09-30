from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        F.sum("unit_price").over(w).alias("cumulative_unit_price"),
        F.avg("quantity").over(w2).alias("quantity_avg_3"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
        F.stddev_samp("unit_price").over(w4).alias("product_id_stddev_samp_unit_price"),
    )
)
