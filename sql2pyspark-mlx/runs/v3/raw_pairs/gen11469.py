from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("orders.status") == "PENDING")
    .select(
        F.col("shipments.id"),
        F.col("shipments.carrier"),
        F.col("shipments.status"),
        F.col("orders.discount"),
        F.col("orders.shipping_fee"),
        F.col("stores.opened_date"),
        F.col("stores.city"),
    )
)
