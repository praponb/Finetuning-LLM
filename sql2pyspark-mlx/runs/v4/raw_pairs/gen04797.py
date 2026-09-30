from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("transactions.id"),
        F.col("transactions.txn_time"),
        F.col("accounts.account_type"),
        F.col("accounts.balance"),
        F.col("customers.is_active"),
    )
    .orderBy(F.col("balance").desc(), F.col("id").desc())
)
