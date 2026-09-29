from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        "store_id",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
