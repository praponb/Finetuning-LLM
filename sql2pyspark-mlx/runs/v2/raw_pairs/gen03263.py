from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "store_id",
        "quantity",
        F.last("store_id").over(w).alias("last_store_id"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
    .orderBy("product_id")
)
