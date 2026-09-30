from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock")

result = (
    df_inventory
    .filter(F.col("product_id") == 7)
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 1)
    .select("id", "product_id", "last_restock", "quantity")
)
