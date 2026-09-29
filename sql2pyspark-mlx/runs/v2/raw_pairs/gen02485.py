from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 5)
    .select("id", "product_id", "quantity", "unit_price")
)
