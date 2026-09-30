from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .filter(F.col("balance") >= 69000)
    .groupBy("customer_id")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("customer_summary"), F.col("customers.id") == F.col("customer_summary.customer_id"), "left")
    .select(F.col("customers.name"), F.col("customer_summary.balance_range"))
)
