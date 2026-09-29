from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_anti")
    .select(F.col("accounts.id"), F.col("accounts.status"), F.col("accounts.balance"))
)
