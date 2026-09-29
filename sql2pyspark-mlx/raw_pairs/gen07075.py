from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.status"), F.col("t.refund_count"))
    .orderBy(F.col("id").desc(), "status")
)
