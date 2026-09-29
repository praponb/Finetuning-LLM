from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("tba.total_count"))
    .orderBy(F.col("total_count").desc(), "id")
    .limit(1)
)
