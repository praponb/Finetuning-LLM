from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        F.sum("quantity").over(w_ordered).alias("cumulative_quantity"),
        F.lag("quantity", 1).over(w2).alias("prev_quantity"),
    )
)
