from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .groupBy("customer_id")
    .agg(F.min("order_date").alias("min_order_date"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(F.col("c.name"), F.col("c.is_active"), F.col("ct.min_order_date"))
    .orderBy("name")
)
