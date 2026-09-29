from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.max("quantity").alias("max_inventory_quantity"))

with_windows = (
    df_inventory
    .filter(F.col("product_id").isin([3, 6, 8]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.sum("quantity").over(w_part).alias("last4_sum_quantity"),
        F.min("quantity").over(w_part).alias("last4_min_quantity"),
        F.max("quantity").over(w_part).alias("last4_max_quantity"),
        F.avg("quantity").over(w_part).alias("last4_avg_quantity"),
    )
)

result = (
    with_windows
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "last4_min_quantity",
        "last4_avg_quantity",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
