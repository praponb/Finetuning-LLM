from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .filter(F.month("last_restock").isin([4, 5, 7]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "product_id",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 5)
    .select("id", "store_id", "last_restock", "product_id")
    .orderBy("last_restock")
)
