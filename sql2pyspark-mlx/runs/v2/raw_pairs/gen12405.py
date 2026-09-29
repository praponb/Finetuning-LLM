from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("channel").isin(["web", "partner"]))
    .groupBy("customer_id")
    .agg(F.min("order_date").alias("min_order_date"))
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("obc"), F.col("c.id") == F.col("obc.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.is_active"),
        F.col("obc.min_order_date"),
    )
    .orderBy("min_order_date")
)
