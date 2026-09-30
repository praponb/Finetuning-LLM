from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "product_id",
        F.min("quantity").over(w).alias("order_id_min_quantity"),
        F.rank().over(w2).alias("rnk"),
    )
)
