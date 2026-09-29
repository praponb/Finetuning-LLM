from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w2 = Window.orderBy("last_restock", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("store_id", "product_id")
w4 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w5 = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 6, 8]))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_5_prior"),
        F.sum("quantity").over(w3).alias("store_id_sum_quantity"),
        F.lead("quantity", 1).over(w4).alias("next_quantity"),
        F.row_number().over(w5).alias("seq_no"),
    )
)
