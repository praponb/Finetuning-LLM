from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .groupBy(F.col("accounts.id"), F.col("accounts.status"))
    .agg(
        F.count(F.col("transactions.id")).alias("transaction_count"),
        F.min(F.col("transactions.amount")).alias("min_amount"),
        F.sum(F.col("transactions.amount")).alias("total_amount"),
    )
    .orderBy(F.col("transaction_count").desc(), "id", "status")
    .limit(10)
)
