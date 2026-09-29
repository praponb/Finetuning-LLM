from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
    )
    .orderBy("product_id")
)
