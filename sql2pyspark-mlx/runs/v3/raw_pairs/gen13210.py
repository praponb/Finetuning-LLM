from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .filter(F.col("quantity") >= 4)
    .select(
        "id",
        "product_id",
        "unit_price",
        "order_id",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
)
