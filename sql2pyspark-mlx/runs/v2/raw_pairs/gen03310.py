from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .filter(F.col("a.status") == "CLOSED")
    .groupBy(F.date_trunc("MONTH", F.col("a.opened_date")).alias("opened_month"))
    .agg(F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"))
)
