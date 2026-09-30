from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .filter(F.col("orders.store_id").isNotNull())
    .join(df_stores.filter(F.col("opened_date") >= "2024-06-01").alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left_anti")
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.store_id"),
        F.col("orders.shipping_fee"),
    )
    .orderBy(F.col("id").desc())
)
