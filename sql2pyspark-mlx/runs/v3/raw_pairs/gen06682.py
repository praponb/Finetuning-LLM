from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .select(
        F.col("shipments.shipped_date"),
        F.col("orders.order_date"),
        F.col("stores.city"),
        F.col("stores.region"),
    )
    .orderBy(F.col("region").desc())
)
