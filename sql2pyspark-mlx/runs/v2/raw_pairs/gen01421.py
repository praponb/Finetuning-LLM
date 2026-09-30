from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("T"), F.col("a.id") == F.col("T.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.balance"), F.col("T.grab_count"))
    .orderBy("grab_count")
)
