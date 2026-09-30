from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.col("customer_id").isin([2, 13]))
    .groupBy("customer_id")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)

result = (
    df_customers.alias("customers")
    .join(accounts_by_customer.alias("accounts_by_customer"), F.col("customers.id") == F.col("accounts_by_customer.customer_id"), "inner")
    .filter(F.col("accounts_by_customer.balance_range") > 71)
    .select(F.col("customers.name"), F.col("accounts_by_customer.balance_range"))
    .orderBy("name")
)
