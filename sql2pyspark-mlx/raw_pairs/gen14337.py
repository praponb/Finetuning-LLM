from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.max("balance").alias("max_balance"))
)

result = (
    df_customers.alias("customers")
    .join(accounts_by_customer.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.max_balance") > 52000)
    .select(F.col("customers.name"), F.col("t.max_balance"))
)
