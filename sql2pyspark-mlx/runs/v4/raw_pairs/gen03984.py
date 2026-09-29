from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price")

result = (
    df_order_items
    .filter(F.col("unit_price") <= 150)
    .select(
        "id",
        "product_id",
        "unit_price",
        "order_id",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 5)
    .select("id", "product_id", "unit_price", "order_id")
    .orderBy("product_id", F.col("unit_price").desc())
)
