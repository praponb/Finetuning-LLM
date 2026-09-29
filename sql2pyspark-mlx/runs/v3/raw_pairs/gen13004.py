from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 5, 6]))
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_3"),
    )
)
