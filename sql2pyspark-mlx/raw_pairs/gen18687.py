from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "outer")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(~(F.col("orders.store_id").isNotNull() | (F.col("orders.store_id") == 13)))
    .select(
        F.col("shipments.id"),
        F.col("shipments.status"),
        F.col("orders.shipping_fee"),
        F.col("orders.channel"),
        F.col("customers.country"),
        F.col("customers.credit_limit"),
    )
    .orderBy("status")
)
