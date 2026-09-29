from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.sum("quantity").over(w_hist).alias("running_sum_quantity"),
        F.count("quantity").over(w_hist).alias("running_count_quantity"),
        F.avg("quantity").over(w_hist).alias("running_avg_quantity"),
    )
)

result = (
    scored
    .select("id", "store_id", "product_id", "quantity", "running_sum_quantity")
    .orderBy("id")
)
