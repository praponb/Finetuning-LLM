from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("status") == "RETURNED")
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.count("*").alias("order_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("t.pending_count"),
        F.col("t.order_count"),
    )
    .orderBy(F.col("pending_count").desc())
)
