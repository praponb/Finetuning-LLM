from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("last_restock"), "product_id").orderBy(F.col("quantity").desc_nulls_last())

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
        F.percent_rank().over(w3).alias("quantity_percent_rank"),
    )
)
