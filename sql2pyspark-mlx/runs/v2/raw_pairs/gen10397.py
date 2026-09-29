from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "outer")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.month(F.col("orders.order_date")).isin([3, 6, 11]))
    .select(
        F.col("shipments.id"),
        F.col("shipments.delivered_date"),
        F.col("orders.shipping_fee"),
        F.col("customers.age"),
        F.col("customers.city"),
    )
)
