from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("account_id") == 35)
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.avg_amount") >= 38)
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("as.unique_merchants"),
        F.col("as.avg_amount"),
    )
)
