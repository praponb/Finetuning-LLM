from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("product_id")

result = (
    df_order_items
    .filter(F.col("order_id") == 30)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
        F.avg("unit_price").over(w2).alias("product_id_avg_unit_price"),
    )
)
