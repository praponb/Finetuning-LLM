from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("last_restock") < "2025-01-01")
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_4"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
)
