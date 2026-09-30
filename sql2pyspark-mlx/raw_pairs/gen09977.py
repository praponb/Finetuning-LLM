from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")

result = (
    df_order_items
    .select(
        "*",
        F.sum("unit_price").over(w).alias("running_unit_price"),
        F.avg("unit_price").over(w2).alias("order_id_avg_unit_price"),
    )
)
