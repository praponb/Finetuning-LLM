from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("last_restock"), "product_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("quantity") > 156)
    .select(
        "id",
        "last_restock",
        "product_id",
        "quantity",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_3"),
    )
)
