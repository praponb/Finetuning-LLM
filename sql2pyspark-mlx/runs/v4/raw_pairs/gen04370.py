from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("account_id") == 27)
    .groupBy("account_id")
    .agg(F.count("*").alias("num_transactions"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("T"), F.col("a.id") == F.col("T.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.account_type"), F.col("T.num_transactions"))
)
