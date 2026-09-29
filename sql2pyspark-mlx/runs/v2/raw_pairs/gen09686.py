from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "right")
    .filter((F.col("orders.amount") <= 1080) & (F.year(F.col("shipments.delivered_date")) == 2024))
    .select(
        F.col("shipments.id"),
        F.col("shipments.status"),
        F.col("shipments.cost"),
        F.col("shipments.delivered_date"),
        F.col("orders.shipping_fee"),
        F.round((F.col("orders.shipping_fee") / F.col("orders.amount")), 2).alias("amount_with_shipping"),
    )
    .orderBy(F.col("id").desc(), F.col("status").desc())
)
