from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
    .select("id", "product_id", "quantity", "last_restock")
    .orderBy("id")
)
