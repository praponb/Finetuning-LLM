from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 7, 8]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "product_id",
        "quantity",
        F.rank().over(w).alias("last_restock_rank"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
