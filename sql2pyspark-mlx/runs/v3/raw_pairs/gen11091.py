from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-03-01", "2024-03-01"))
    .select(
        "id",
        "store_id",
        "quantity",
        "product_id",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("id").desc())
)
