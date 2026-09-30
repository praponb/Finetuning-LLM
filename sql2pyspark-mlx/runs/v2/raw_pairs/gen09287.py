from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .select(
        F.col("orders.store_id"),
        F.col("orders.order_date"),
        F.col("stores.region"),
        F.col("stores.store_name"),
    )
    .orderBy(F.col("store_id").desc())
)
