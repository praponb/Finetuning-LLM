from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.sum("quantity").over(w).alias("running_sum_quantity"),
        F.count("quantity").over(w).alias("running_count_quantity"),
        F.avg("quantity").over(w).alias("running_avg_quantity"),
        F.min("quantity").over(w).alias("running_min_quantity"),
    )
)
