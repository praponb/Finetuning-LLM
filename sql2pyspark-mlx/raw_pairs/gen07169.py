from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 6]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.max("quantity").over(w).alias("last7_max_quantity"),
        F.sum("quantity").over(w).alias("last7_sum_quantity"),
        F.count("quantity").over(w).alias("last7_count_quantity"),
        F.min("quantity").over(w).alias("last7_min_quantity"),
    )
)
