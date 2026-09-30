from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "store_id",
        F.rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "product_id", "last_restock", "store_id")
    .orderBy(F.col("store_id").desc())
)
