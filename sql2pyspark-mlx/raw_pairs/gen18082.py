from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .groupBy(F.col("accounts.id"), F.col("accounts.account_type"))
    .agg(F.count(F.col("transactions.id")).alias("transaction_count"))
    .orderBy("account_type", F.col("id").desc())
)
