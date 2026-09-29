from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("balance") < 72000)
    .groupBy("customer_id")
    .agg(F.min("balance").alias("min_balance"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.min_balance") > 75000)
    .select(F.col("c.country"), F.col("cs.min_balance"))
)
