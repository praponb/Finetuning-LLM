from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "last_restock")).orderBy("quantity", "id")
w_part = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        F.ntile(5).over(w_running).alias("quantity_bucket"),
        F.lead("quantity", 2, 0).over(w2).alias("next_quantity"),
        F.last("quantity", ignorenulls=True).over(w_part).alias("last_known_quantity"),
    )
)
