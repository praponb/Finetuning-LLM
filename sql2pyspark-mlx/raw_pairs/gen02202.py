from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.partitionBy(F.year("last_restock"), "product_id").orderBy("quantity", "id")
w3 = Window.partitionBy(F.year("last_restock"), "store_id").orderBy("last_restock", "id").rowsBetween(-2, -1)
w4 = Window.orderBy("quantity", "id")

result = (
    df_inventory
    .filter(F.col("quantity").between(63, 137))
    .select(
        "id",
        "store_id",
        F.sum("quantity").over(w).alias("product_id_sum_quantity"),
        F.lag("quantity", 1, 0).over(w2).alias("prev_quantity"),
        F.avg("quantity").over(w3).alias("quantity_avg_2_prior"),
        F.lag("quantity", 2).over(w4).alias("prev_quantity_2"),
    )
)
