from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "outer")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.city").isin(["Tokyo", "London", "Berlin"]) & (F.col("orders.amount") <= 1510))
    .select(
        F.col("shipments.order_id"),
        F.col("orders.channel"),
        F.col("customers.is_active"),
    )
    .orderBy("channel")
)
