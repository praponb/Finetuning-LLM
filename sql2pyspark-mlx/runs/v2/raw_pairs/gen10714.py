from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .groupBy("customer_id")
    .agg(F.countDistinct("amount").alias("unique_stores"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.unique_stores") >= 8)
    .select(F.col("c.credit_limit"), F.col("c.email"), F.col("ct.unique_stores"))
)
