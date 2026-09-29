from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(F.col("a.status"), F.col("as.shell_count"))
    .orderBy("shell_count", F.col("status").desc())
)
