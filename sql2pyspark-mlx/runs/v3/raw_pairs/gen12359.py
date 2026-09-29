from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .filter(F.col("last_restock") > "2023-08-07")
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 5)
    .select("id", "store_id", "quantity", "last_restock")
    .orderBy(F.col("quantity").desc())
)
