from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .filter(F.year(F.col("accounts.opened_date")) == 2025)
    .select(F.col("accounts.balance"), F.col("transactions.txn_time"))
)
