from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("quantity")
w_running = Window.partitionBy("store_id").orderBy("last_restock", "id")

with_windows = (
    df_inventory
    .select(
        "*",
        F.stddev_samp("quantity").over(w_ordered).alias("store_id_stddev_samp_quantity"),
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
        F.dense_rank().over(w_recent).alias("quantity_dense_rank"),
        F.lead("quantity", 2).over(w_running).alias("next_quantity"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "store_id_stddev_samp_quantity",
        "quantity_dense_rank",
        "next_quantity",
    )
    .orderBy(F.col("last_restock").desc_nulls_last())
)
