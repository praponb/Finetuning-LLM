from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.sum("quantity").over(w).alias("running_quantity"),
    )
    .orderBy(F.col("running_quantity").desc())
)
