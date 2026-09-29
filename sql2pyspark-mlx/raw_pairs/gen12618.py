from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.amount_range") >= 45)
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("t.amount_range"))
)
