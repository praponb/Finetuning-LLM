from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .filter(F.col("last_restock") > "2024-06-01")
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id",
        "last_restock",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.dense_rank().over(w2).alias("dense_rnk_2"),
    )
    .orderBy(F.col("id").desc(), "product_id")
)
