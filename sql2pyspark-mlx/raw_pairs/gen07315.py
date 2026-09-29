from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock")

result = (
    df_inventory
    .filter(F.col("quantity") <= 97)
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 5)
    .orderBy("id", F.col("last_restock").desc())
)
