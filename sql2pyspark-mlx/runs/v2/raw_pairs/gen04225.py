from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter(F.hour(F.col("transactions.txn_time")).between(8, 11))
    .select(
        F.col("accounts.status"),
        F.col("accounts.account_type"),
        F.col("accounts.balance"),
        F.col("transactions.account_id"),
    )
    .orderBy(F.col("account_id").desc())
)
