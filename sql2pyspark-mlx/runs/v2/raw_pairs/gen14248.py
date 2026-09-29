from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id")

result = (
    df_order_items
    .filter(F.col("order_id") == 28)
    .select(
        "id",
        "order_id",
        "quantity",
        "product_id",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.count("*").over(w2).alias("product_id_count"),
    )
    .orderBy(F.col("product_id").desc())
)
