from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id")

result = (
    df_order_items
    .filter(F.col("quantity") != 6)
    .select(
        "id",
        "product_id",
        "quantity",
        "order_id",
        F.first("order_id").over(w).alias("first_order_id"),
        F.max("quantity").over(w2).alias("order_id_max_quantity"),
    )
)
