from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.col("customer_id").isin([2, 4, 7, 30]))
    .groupBy("customer_id")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.balance_range") > 24)
    .select(F.col("c.city"), F.col("c.country"), F.col("t.balance_range"))
)
