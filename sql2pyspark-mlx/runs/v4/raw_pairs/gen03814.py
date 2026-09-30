from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("discount") >= 0.31)
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"))
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("obc"), F.col("c.id") == F.col("obc.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.name"), F.col("obc.returned_count"))
    .orderBy("name")
)
