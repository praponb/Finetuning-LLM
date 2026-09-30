from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 5, 8, 10]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.row_number().over(w2).alias("last_restock_row"),
    )
    .orderBy(F.col("quantity").desc())
)
