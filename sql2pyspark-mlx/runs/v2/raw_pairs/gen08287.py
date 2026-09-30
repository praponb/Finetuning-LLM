from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("account_summary"), F.col("accounts.id") == F.col("account_summary.account_id"), "inner")
    .select(
        F.col("accounts.status"),
        F.col("accounts.customer_id"),
        F.col("account_summary.min_amount"),
        F.col("account_summary.stddev_amount"),
    )
    .orderBy("stddev_amount")
)
