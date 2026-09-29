from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.dense_rank().over(w).alias("last_restock_dense_rank"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
    .orderBy("last_restock", F.col("store_id").desc())
)
