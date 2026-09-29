from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "product_id", "unit_price", "quantity")
    .orderBy(F.col("unit_price").desc())
)
