from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .filter(F.col("last_restock") > "2024-09-06")
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "store_id", "quantity", "last_restock")
)
