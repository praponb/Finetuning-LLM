from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 6]))
    .select(
        "id",
        "last_restock",
        F.dense_rank().over(w).alias("last_restock_dense_rank"),
    )
    .orderBy(F.col("last_restock_dense_rank").desc())
)
