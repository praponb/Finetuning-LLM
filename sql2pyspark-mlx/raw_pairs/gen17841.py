from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.hour("txn_time").between(11, 16))
    .groupBy("account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("as.avg_amount"),
        F.col("as.shell_count"),
    )
    .orderBy(F.col("shell_count").desc(), "id")
    .limit(5)
)
