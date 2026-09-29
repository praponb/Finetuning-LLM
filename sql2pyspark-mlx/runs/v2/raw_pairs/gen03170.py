from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .filter(F.col("at.avg_amount") > 3100)
    .select(F.col("a.customer_id"), F.col("at.avg_amount"))
)
