from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("accounts.balance") < 74000)
    .select(
        F.col("transactions.id"),
        F.col("transactions.txn_type"),
        F.col("transactions.account_id"),
        F.col("accounts.account_type"),
        F.col("accounts.balance"),
        F.col("accounts.customer_id"),
    )
    .orderBy(F.col("account_id").desc())
)
