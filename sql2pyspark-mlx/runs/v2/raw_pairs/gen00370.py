from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("merchant") == "7-Eleven")
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("account_stats"), F.col("accounts.id") == F.col("account_stats.account_id"), "left")
    .select(
        F.col("accounts.id"),
        F.col("accounts.status"),
        F.col("account_stats.total_amount"),
        F.col("account_stats.max_amount"),
    )
    .orderBy("total_amount")
)
