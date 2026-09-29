from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price")

result = (
    df_order_items
    .filter(F.col("quantity") >= 7)
    .select(
        "id",
        "product_id",
        "unit_price",
        F.dense_rank().over(w).alias("unit_price_dense_rank"),
    )
)
