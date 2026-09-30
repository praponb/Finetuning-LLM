from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .groupBy("customer_id")
    .agg((F.max("discount") - F.min("discount")).alias("discount_range"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.credit_limit"), F.col("ct.discount_range"))
)
