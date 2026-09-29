from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("amount") < 2000)
    .groupBy("account_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.amount_range") >= 71)
    .select(F.col("a.id"), F.col("a.status"), F.col("as.amount_range"))
)
