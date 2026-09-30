from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 5, 6]))
    .select(
        "id",
        "product_id",
        "store_id",
        "last_restock",
        "quantity",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
    )
    .orderBy("last_restock")
)
