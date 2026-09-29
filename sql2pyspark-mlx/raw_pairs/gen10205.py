from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_inventory
    .select(
        "*",
        F.avg("quantity").over(w_hist).alias("running_avg_quantity"),
        F.count("quantity").over(w_hist).alias("running_count_quantity"),
        F.min("quantity").over(w_hist).alias("running_min_quantity"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "running_avg_quantity",
        "running_count_quantity",
    )
    .orderBy(F.col("quantity").desc_nulls_first(), "running_avg_quantity")
)
