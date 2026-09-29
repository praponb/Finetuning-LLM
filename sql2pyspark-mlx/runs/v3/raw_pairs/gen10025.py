from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("account_stats"), F.col("accounts.id") == F.col("account_stats.account_id"), "left")
    .select(
        F.col("accounts.account_type"),
        F.col("accounts.status"),
        F.col("account_stats.max_amount"),
    )
    .orderBy(F.col("status").desc(), F.col("max_amount").desc())
)
