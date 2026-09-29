from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "left")
    .filter(F.lower(F.col("customers.name")).like("%aro%"))
    .select(
        F.col("transactions.id"),
        F.col("transactions.merchant"),
        F.col("transactions.txn_time"),
        F.col("accounts.status"),
        F.col("customers.credit_limit"),
        F.col("customers.city"),
    )
)
