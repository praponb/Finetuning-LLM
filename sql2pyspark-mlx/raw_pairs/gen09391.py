from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("quantity").desc(), "id")
w_running = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")
w_grp = Window.partitionBy(F.year("last_restock"), "store_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

with_windows = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.ntile(3).over(w_recent).alias("quantity_bucket"),
        F.row_number().over(w_running).alias("event_seq"),
        F.row_number().over(w_running).alias("row_seq"),
        F.avg("quantity").over(w_grp).alias("quantity_avg_2"),
    )
)

result = (
    with_windows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity_bucket",
        "event_seq",
        "row_seq",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("quantity_bucket").desc())
)
