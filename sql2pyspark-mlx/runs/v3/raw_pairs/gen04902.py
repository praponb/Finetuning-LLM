from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("account_summary"), F.col("accounts.id") == F.col("account_summary.account_id"), "left")
    .select(
        F.col("accounts.customer_id"),
        F.col("accounts.opened_date"),
        F.col("account_summary.avg_amount"),
    )
    .orderBy("customer_id")
)
