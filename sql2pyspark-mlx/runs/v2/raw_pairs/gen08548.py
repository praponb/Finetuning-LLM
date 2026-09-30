from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.store_id").isNull())
    .select(
        F.col("shipments.delivered_date"),
        F.col("shipments.carrier"),
        F.col("shipments.order_id"),
        F.col("orders.channel"),
    )
    .orderBy("carrier", "delivered_date")
)
