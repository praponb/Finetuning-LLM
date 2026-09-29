from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.balance"), F.col("t.refund_amount"))
    .orderBy("refund_amount")
)
