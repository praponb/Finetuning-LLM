from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.min("order_date").alias("min_order_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("t.first_order_date"),
        F.col("t.min_order_date"),
    )
    .orderBy("min_order_date", "name")
)
