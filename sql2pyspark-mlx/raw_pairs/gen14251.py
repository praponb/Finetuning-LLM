from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.avg("quantity").over(w).alias("store_id_avg_quantity"),
        F.ntile(10).over(w2).alias("quantity_bucket"),
    )
)
