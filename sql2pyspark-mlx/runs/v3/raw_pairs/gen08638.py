from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.filter(F.col("order_id") == 5).alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "left_semi")
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("orders.customer_id"),
        F.col("orders.channel"),
    )
    .orderBy(F.col("channel").desc())
)
