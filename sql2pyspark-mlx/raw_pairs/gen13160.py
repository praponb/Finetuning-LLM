from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 6]))
    .select(
        "id",
        "store_id",
        "quantity",
        "product_id",
        F.first("product_id").over(w).alias("first_product_id"),
        F.min("quantity").over(w2).alias("store_id_min_quantity"),
    )
    .orderBy("store_id_min_quantity", F.col("first_product_id").desc())
)
