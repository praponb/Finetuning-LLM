from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "store_id",
        "last_restock",
        F.rank().over(w).alias("last_restock_rank"),
    )
)
