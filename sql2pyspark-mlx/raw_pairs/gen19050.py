from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "outer")
    .filter(F.col("stores.region") == "West")
    .select(
        F.col("shipments.shipped_date"),
        F.col("shipments.status"),
        F.col("orders.channel"),
        F.col("orders.order_date"),
        F.col("stores.region"),
        F.col("stores.sqft"),
    )
)
