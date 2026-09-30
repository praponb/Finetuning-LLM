from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.filter(F.col("delivered_date") >= "2025-05-01").alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "left_semi")
    .select(F.col("orders.id"), F.col("orders.amount"), F.col("orders.shipping_fee"))
    .orderBy(F.col("shipping_fee").desc(), F.col("id").desc())
)
