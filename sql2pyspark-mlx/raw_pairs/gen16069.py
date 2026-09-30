from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_first())
amount_by_store_id = df_orders.groupBy("store_id").agg(F.min("amount").alias("min_order_amount"))

metrics = (
    df_inventory
    .select(
        "*",
        F.min("quantity").over(w).alias("store_id_min_quantity"),
        F.cume_dist().over(w2).alias("quantity_cume_dist"),
    )
)

result = (
    metrics
    .join(amount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "quantity_cume_dist",
        F.col("min_order_amount"),
    )
    .orderBy(F.col("min_order_amount").desc_nulls_last())
)
