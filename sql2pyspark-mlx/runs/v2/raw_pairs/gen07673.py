from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.account_id").isin([20, 25, 26]))
    .groupBy(F.col("a.status"))
    .agg(F.count("*").alias("transaction_count"))
    .orderBy("status")
)
