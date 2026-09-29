from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "quantity",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_3"),
    )
    .orderBy("quantity_moving_avg_3")
)
