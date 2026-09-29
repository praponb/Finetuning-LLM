from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity")
w2 = Window.partitionBy("product_id", "order_id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("product_id")
w5 = Window.orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "*",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4).alias("quantity_share"),
        F.sum("unit_price").over(w3).alias("order_id_sum_unit_price"),
        F.min("unit_price").over(w4).alias("product_id_min_unit_price"),
        F.ntile(3).over(w5).alias("quantity_bucket"),
    )
)
