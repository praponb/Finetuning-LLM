from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("product_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        F.sum("unit_price").over(w).alias("order_id_sum_unit_price"),
        F.sum("unit_price").over(w2).alias("product_id_sum_unit_price"),
    )
)
