from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.max("quantity").over(w_part).alias("running_max_quantity"),
        F.min("quantity").over(w_part).alias("running_min_quantity"),
        F.sum("quantity").over(w_part).alias("running_sum_quantity"),
        F.avg("quantity").over(w_part_2).alias("quantity_avg_3"),
    )
)
