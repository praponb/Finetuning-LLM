from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "outer")
    .filter(F.col("orders.status").isin(["COMPLETED", "RETURNED"]) | F.month(F.col("shipments.shipped_date")).isin([3, 7, 9]))
    .select(
        F.col("shipments.id"),
        F.col("shipments.carrier"),
        F.col("shipments.shipped_date"),
        F.col("orders.customer_id"),
        F.col("stores.region"),
    )
)
