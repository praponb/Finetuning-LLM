from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("accounts.balance") > 40000)
    .groupBy(F.col("accounts.id"), F.col("accounts.status"))
    .agg(
        F.count(F.col("transactions.id")).alias("transaction_count"),
        F.sum(F.col("transactions.amount")).alias("total_amount"),
        F.countDistinct(F.col("transactions.amount")).alias("unique_merchants"),
    )
    .filter(F.col("transaction_count") >= 4)
)
