from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter((F.col("orders.shipping_fee") <= 32) | (F.col("orders.discount") >= 0.1))
    .select(
        F.col("shipments.id"),
        F.col("shipments.carrier"),
        F.col("shipments.shipped_date"),
        F.col("shipments.cost"),
        F.col("orders.amount"),
        F.col("orders.order_date"),
    )
)
