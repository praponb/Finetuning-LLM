from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 5, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "quantity",
        F.count("*").over(w).alias("store_id_count"),
        (F.col("quantity") - F.lag("quantity", 1).over(w2)).alias("quantity_change"),
    )
)
