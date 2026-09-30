from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.amount") >= 2400)
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(F.count(F.col("t.id")).alias("transaction_count"))
)
