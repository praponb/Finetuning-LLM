from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.filter(F.col("credit_limit").between(5800, 7400)).alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left_semi")
    .select(F.col("accounts.id"), F.col("accounts.balance"))
)
