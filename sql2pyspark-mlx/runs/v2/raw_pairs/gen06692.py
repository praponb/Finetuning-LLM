from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("account_stats"), F.col("accounts.id") == F.col("account_stats.account_id"), "left")
    .select(
        F.col("accounts.opened_date"),
        F.col("account_stats.shell_count"),
        F.col("account_stats.max_amount"),
    )
    .orderBy(F.col("opened_date").desc())
)
