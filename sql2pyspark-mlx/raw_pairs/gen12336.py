from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.max("quantity").alias("max_inventory_quantity"))

with_windows = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        F.count("quantity").over(w).alias("last7_count_quantity"),
        F.avg("quantity").over(w).alias("last7_avg_quantity"),
        F.min("quantity").over(w).alias("last7_min_quantity"),
        F.ntile(4).over(w2).alias("quantity_bucket"),
    )
)

result = (
    with_windows
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "last7_count_quantity",
        "last7_min_quantity",
        "quantity_bucket",
        F.col("max_inventory_quantity"),
    )
    .orderBy("id", "store_id")
)
