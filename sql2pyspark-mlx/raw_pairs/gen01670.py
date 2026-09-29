from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .filter((F.col("orders.shipping_fee") < 22) & F.col("shipments.status").isin(["IN_TRANSIT", "DELIVERED"]))
    .select(F.col("orders.store_id"), F.col("orders.status"), F.col("shipments.cost"))
)
