from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("stores.region").isNotNull())
    .select(
        F.col("shipments.id"),
        F.col("shipments.status"),
        F.col("orders.channel"),
        F.col("stores.region"),
        F.col("stores.store_name"),
    )
)
