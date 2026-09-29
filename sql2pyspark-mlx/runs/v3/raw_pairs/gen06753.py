from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 6, 9]))
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "product_id", "last_restock", "quantity")
)
