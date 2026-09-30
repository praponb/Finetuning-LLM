from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("discount").between(0.12, 0.31))
    .groupBy("customer_id")
    .agg(F.min("order_date").alias("min_order_date"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.city"), F.col("cs.min_order_date"))
    .orderBy(F.col("min_order_date").desc())
)
