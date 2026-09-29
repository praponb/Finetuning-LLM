from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter(~((F.col("transactions.txn_type") == "REFUND") & (F.col("accounts.balance") < 74000)))
    .select(
        F.col("transactions.id"),
        F.col("transactions.txn_time"),
        F.col("transactions.is_flagged"),
        F.col("accounts.balance"),
        F.col("accounts.account_type"),
    )
)
