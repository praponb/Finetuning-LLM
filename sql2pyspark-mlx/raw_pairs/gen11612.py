from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_7"),
    )
    .orderBy(F.col("last_restock").desc(), F.col("product_id").desc())
)
