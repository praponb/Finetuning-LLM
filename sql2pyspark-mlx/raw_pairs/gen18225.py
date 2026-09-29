from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("account_id") == 9)
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.balance"), F.col("tba.unique_txn_types"))
)
