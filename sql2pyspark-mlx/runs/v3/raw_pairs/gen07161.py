from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(F.col("a.id"))
    .agg(F.count(F.col("t.id")).alias("transaction_count"))
    .filter(F.col("transaction_count") >= 3)
)
