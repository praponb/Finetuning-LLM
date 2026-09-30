from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "left")
    .select(F.col("accounts.id"), F.col("accounts.balance"), F.col("t.min_amount"))
    .orderBy(F.col("id").desc())
)
