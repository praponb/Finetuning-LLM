from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-04-01", "2023-08-08"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "quantity",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_7"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
