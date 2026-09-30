from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "outer")
    .filter(F.col("accounts.opened_date") >= "2024-06-01")
    .groupBy(F.col("accounts.status"))
    .agg(F.max(F.col("transactions.txn_time")).alias("last_txn_time"))
    .orderBy("status")
)
