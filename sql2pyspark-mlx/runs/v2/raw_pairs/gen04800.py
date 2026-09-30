from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 5]))
    .select(
        "id",
        "store_id",
        "quantity",
        "product_id",
        F.max("quantity").over(w).alias("store_id_max_quantity"),
        F.avg("quantity").over(w2).alias("product_id_avg_quantity"),
    )
)
