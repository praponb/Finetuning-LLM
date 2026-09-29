from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.rank().over(w).alias("quantity_rank"),
        F.ntile(4).over(w2).alias("quantity_bucket"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
    )
)
