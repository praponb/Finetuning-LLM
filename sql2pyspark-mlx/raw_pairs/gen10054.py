from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .groupBy("customer_id")
    .agg(F.max("balance").alias("max_balance"))
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("abc"), F.col("c.id") == F.col("abc.customer_id"), "inner")
    .filter(F.col("abc.max_balance") > 48000)
    .select(F.col("c.age"), F.col("c.credit_limit"), F.col("abc.max_balance"))
)
