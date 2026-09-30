from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")
w3 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

with_windows = (
    df_inventory
    .withColumn("product_id_avg_quantity", F.avg("quantity").over(w))
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w2))
    .withColumn("running_quantity", F.sum("quantity").over(w3))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w4))
)

result = (
    with_windows
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "product_id_avg_quantity",
        "running_quantity",
        F.col("min_inventory_quantity"),
    )
    .orderBy("running_quantity")
)
