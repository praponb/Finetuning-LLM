from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .groupBy("customer_id")
    .agg(F.sum("discount").alias("total_discount"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.total_discount") >= 2.0)
    .select(F.col("c.id"), F.col("c.city"), F.col("cs.total_discount"))
)
