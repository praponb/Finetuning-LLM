from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.txn_time") < "2024-03-15")
    .groupBy(F.col("a.id"), F.col("a.status"))
    .agg(F.count(F.col("t.id")).alias("transaction_count"))
)
