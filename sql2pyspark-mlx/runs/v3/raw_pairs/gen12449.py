from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("quantity") <= 210)
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "quantity",
        F.sum("quantity").over(w).alias("running_quantity"),
    )
)
