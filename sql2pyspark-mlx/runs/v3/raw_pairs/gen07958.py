from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.rank().over(w).alias("last_restock_rank"),
    )
)
