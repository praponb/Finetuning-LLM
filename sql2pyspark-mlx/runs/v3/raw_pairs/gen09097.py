from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("shipments.order_id").isin([37, 39]) & (F.col("orders.shipping_fee") < 31))
    .select(
        F.col("shipments.delivered_date"),
        F.col("shipments.order_id"),
        F.col("orders.status"),
    )
)
