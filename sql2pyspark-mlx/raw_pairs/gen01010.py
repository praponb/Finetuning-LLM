from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "order_id",
        "quantity",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
        F.max("quantity").over(w2).alias("product_id_max_quantity"),
    )
    .orderBy(F.col("id").desc(), F.col("product_id").desc())
)
