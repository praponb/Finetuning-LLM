from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2023-11-04")
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id",
        "last_restock",
        F.row_number().over(w).alias("rn"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
)
