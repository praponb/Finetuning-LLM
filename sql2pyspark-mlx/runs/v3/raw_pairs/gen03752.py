from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 5]))
    .select(
        "id",
        "store_id",
        "quantity",
        "product_id",
        F.min("quantity").over(w).alias("store_id_min_quantity"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("product_id").desc(), "store_id_min_quantity")
)
