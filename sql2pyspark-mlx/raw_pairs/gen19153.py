from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy(F.year("last_restock"), "product_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 6]))
    .select(
        "id",
        "product_id",
        "quantity",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
        F.avg("quantity").over(w2).alias("quantity_avg_3"),
        F.lead("quantity", 1).over(w3).alias("next_quantity"),
    )
)
