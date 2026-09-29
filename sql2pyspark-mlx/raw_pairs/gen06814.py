from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "store_id",
        "quantity",
        F.row_number().over(w).alias("rn"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
)
