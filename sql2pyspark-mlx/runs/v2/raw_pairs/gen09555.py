from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("T"), F.col("a.id") == F.col("T.account_id"), "inner")
    .filter(F.col("T.row_count") >= 4)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("T.row_count"),
    )
    .orderBy(F.col("row_count").desc(), "id")
    .limit(10)
)
