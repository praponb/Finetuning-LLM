from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("accounts.customer_id") == 11)
    .groupBy(F.col("accounts.account_type"))
    .agg(F.count(F.col("transactions.id")).alias("transaction_count"))
    .orderBy(F.col("account_type").desc(), "transaction_count")
)
