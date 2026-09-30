from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(~F.col("merchant").isin(["Amazon", "Starbucks", "Grab"]))
    .groupBy("account_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.amount_range") >= 26)
    .select(F.col("a.status"), F.col("a.balance"), F.col("t.amount_range"))
    .orderBy("status")
)
