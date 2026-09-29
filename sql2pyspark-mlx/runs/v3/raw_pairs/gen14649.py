from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "product_id", "last_restock", "quantity")
)
