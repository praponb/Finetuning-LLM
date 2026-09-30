from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")

result = (
    df_inventory
    .filter(F.col("quantity") == 157)
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 5)
    .select("id", "product_id", "quantity", "last_restock")
)
