from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "outer")
    .select(
        F.col("transactions.id"),
        F.col("transactions.is_flagged"),
        F.col("transactions.txn_type"),
        F.col("accounts.balance"),
        F.col("customers.city"),
    )
)
