from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .filter(F.year("last_restock") >= 2023)
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .orderBy("quantity")
)
