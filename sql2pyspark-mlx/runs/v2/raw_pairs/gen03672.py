from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity")

result = (
    df_order_items
    .filter(F.col("unit_price") < 270)
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        F.ntile(3).over(w).alias("quartile"),
        F.dense_rank().over(w2).alias("quantity_dense_rank"),
    )
    .orderBy(F.col("quantity_dense_rank").desc(), F.col("quantity").desc())
)
