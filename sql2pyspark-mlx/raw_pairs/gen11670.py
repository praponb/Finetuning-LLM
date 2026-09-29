from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "last_restock"), "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "quantity",
        "last_restock",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.last("quantity", ignorenulls=True).over(w2).alias("last_known_quantity"),
    )
)
