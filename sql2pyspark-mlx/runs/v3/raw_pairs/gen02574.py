from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("stores.city"),
        F.col("stores.region"),
    )
    .orderBy("status")
)
