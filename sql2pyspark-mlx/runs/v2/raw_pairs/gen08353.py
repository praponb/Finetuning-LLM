from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("last_restock").desc())
w2 = Window.partitionBy("product_id").orderBy("last_restock")

result = (
    df_inventory
    .select(
        "id",
        "last_restock",
        "product_id",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.dense_rank().over(w2).alias("last_restock_dense_rank"),
    )
    .orderBy(F.col("id").desc())
)
