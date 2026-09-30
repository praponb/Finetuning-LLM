from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "product_id",
        "unit_price",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
        F.ntile(3).over(w2).alias("bucket"),
    )
)
