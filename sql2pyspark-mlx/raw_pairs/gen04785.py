from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("account_stats"), F.col("accounts.id") == F.col("account_stats.account_id"), "inner")
    .filter(F.col("account_stats.total_amount") >= 21000)
    .select(
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
        F.col("account_stats.amount_range"),
        F.col("account_stats.total_amount"),
    )
)
