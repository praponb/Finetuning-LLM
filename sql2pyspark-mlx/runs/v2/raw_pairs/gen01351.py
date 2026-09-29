from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "right")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "right")
    .select(
        F.col("transactions.id"),
        F.col("transactions.account_id"),
        F.col("accounts.customer_id"),
        F.col("customers.age"),
        F.col("customers.credit_limit"),
    )
)
