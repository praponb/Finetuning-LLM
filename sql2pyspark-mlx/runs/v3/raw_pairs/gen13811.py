from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2023-06-24")
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.sum("quantity").over(w).alias("running_quantity"),
    )
)
