from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "order_id",
        "unit_price",
        F.sum("quantity").over(w).alias("product_id_sum_quantity"),
        F.round(F.avg("unit_price").over(w2), 2).alias("unit_price_moving_avg_7"),
    )
    .orderBy(F.col("quantity").desc())
)
