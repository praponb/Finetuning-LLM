from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .filter(F.col("tba.amount_range") > 54)
    .select(F.col("a.status"), F.col("tba.amount_range"))
)
