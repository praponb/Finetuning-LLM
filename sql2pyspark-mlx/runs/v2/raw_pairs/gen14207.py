from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 6]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_4"),
    )
    .orderBy(F.col("quantity_moving_avg_4").desc())
)
