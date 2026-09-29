from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .filter(F.col("amount") > 560)
    .groupBy("customer_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(F.col("c.age"), F.col("c.email"), F.col("ct.max_amount"))
)
