from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("account_id").isin([10, 11]))
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .select(F.col("a.status"), F.col("A.refund_amount"))
    .orderBy(F.col("refund_amount").desc())
)
