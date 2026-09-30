from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.is_flagged"))
    .groupBy(F.col("a.id"))
    .agg(F.count(F.col("t.id")).alias("transaction_count"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
