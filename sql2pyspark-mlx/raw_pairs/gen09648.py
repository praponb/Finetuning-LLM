from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("A.refund_count"))
    .orderBy("refund_count")
)
