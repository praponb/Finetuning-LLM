from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .filter(F.col("quantity") != 79)
    .select(
        "id",
        "product_id",
        "last_restock",
        "store_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .orderBy(F.col("store_id").desc())
)
