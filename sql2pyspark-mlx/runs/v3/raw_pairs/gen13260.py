from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("order_date") >= "2024-05-01")
    .groupBy("customer_id")
    .agg(F.min("order_date").alias("min_order_date"))
)

result = (
    df_customers.alias("customers")
    .join(orders_by_customer.alias("orders_by_customer"), F.col("customers.id") == F.col("orders_by_customer.customer_id"), "left")
    .select(F.col("customers.name"), F.col("orders_by_customer.min_order_date"))
    .orderBy(F.col("min_order_date").desc())
)
