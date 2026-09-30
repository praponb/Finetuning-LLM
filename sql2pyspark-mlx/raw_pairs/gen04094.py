from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        F.min("quantity").over(w_running).alias("running_min_quantity"),
        F.sum("quantity").over(w_running).alias("running_sum_quantity"),
        F.max("quantity").over(w_running).alias("running_max_quantity"),
    )
)
