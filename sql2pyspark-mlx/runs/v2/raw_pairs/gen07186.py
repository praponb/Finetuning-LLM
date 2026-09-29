from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .groupBy("customer_id")
    .agg(F.max("order_date").alias("latest_order_date"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.is_active"), F.col("t.latest_order_date"))
)
