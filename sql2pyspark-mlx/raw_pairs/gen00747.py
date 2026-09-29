from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter(F.col("transactions.merchant").isin(["Lazada", "Amazon", "Shell"]))
    .groupBy(F.col("accounts.id"))
    .agg(
        F.count(F.col("transactions.id")).alias("transaction_count"),
        F.sum(F.col("transactions.amount")).alias("total_amount"),
    )
    .orderBy(F.col("transaction_count").desc())
)
