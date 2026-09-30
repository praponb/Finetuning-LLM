from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "product_id",
        F.max("quantity").over(w).alias("store_id_max_quantity"),
        F.ntile(3).over(w2).alias("bucket"),
    )
    .orderBy(F.col("quantity").desc(), "product_id")
)
