from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "store_id",
        "last_restock",
        "quantity",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_3"),
        F.lag("quantity", 2, 0).over(w2).alias("prev_quantity"),
    )
)
