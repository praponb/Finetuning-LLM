from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("status").isin(["CANCELLED", "PENDING", "SHIPPED"]))
    .groupBy("customer_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("O"), F.col("c.id") == F.col("O.customer_id"), "inner")
    .filter(F.col("O.total_count") > 9)
    .select(F.col("c.id"), F.col("c.email"), F.col("O.total_count"))
    .orderBy("total_count", F.col("id").desc())
)
