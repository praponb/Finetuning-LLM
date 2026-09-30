from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("amount").between(3300, 3900))
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(F.col("a.status"), F.col("a.opened_date"), F.col("as.total_amount"))
    .orderBy("status")
)
