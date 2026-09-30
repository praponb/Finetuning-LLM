from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.filter(F.col("txn_time") >= "2023-09-12").alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_anti")
    .select(F.col("accounts.id"), F.col("accounts.balance"))
)
