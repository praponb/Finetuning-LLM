from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.account_id").isin([4, 23]))
    .groupBy(F.col("a.status"))
    .agg(F.count(F.col("t.id")).alias("transaction_count"))
)
