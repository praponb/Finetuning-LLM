from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter(F.col("transactions.merchant") == "Grab")
    .select(
        F.col("accounts.status"),
        F.col("transactions.account_id"),
        F.col("transactions.txn_time"),
        F.col("transactions.is_flagged"),
    )
)
