from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "right")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("orders.customer_id").isNotNull())
    .select(
        F.col("shipments.id"),
        F.col("shipments.shipped_date"),
        F.col("orders.channel"),
        F.col("orders.order_date"),
        F.col("customers.credit_limit"),
    )
)
