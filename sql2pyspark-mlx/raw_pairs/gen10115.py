from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        "product_id",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_7"),
        F.sum("quantity").over(w2).alias("product_id_sum_quantity"),
    )
)
