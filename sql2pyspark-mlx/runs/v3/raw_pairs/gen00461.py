from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.col("shipments.cost") >= 62)
    .select(
        F.col("shipments.shipped_date"),
        F.col("shipments.carrier"),
        F.col("orders.store_id"),
        F.col("stores.region"),
        F.col("stores.opened_date"),
    )
)
