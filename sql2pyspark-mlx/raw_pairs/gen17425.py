from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .select(
        F.col("shipments.id"),
        F.col("shipments.shipped_date"),
        F.col("orders.discount"),
        F.col("orders.shipping_fee"),
        F.col("orders.status"),
    )
    .orderBy("discount", "status")
)
