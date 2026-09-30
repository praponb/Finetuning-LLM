from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.partitionBy(F.year("last_restock")).orderBy("quantity", "id")

with_windows = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.avg("quantity").over(w).alias("quantity_avg_6"),
        F.ntile(3).over(w_recent).alias("quantity_bucket"),
    )
)

result = with_windows.select("id", "product_id", "quantity_avg_6", "quantity_bucket")
