from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .filter(F.col("A.avg_amount") >= 47)
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("A.avg_amount"))
)
