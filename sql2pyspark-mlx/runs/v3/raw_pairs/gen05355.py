from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.avg_amount") > 74)
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("t.refund_count"),
        F.col("t.avg_amount"),
    )
    .orderBy("avg_amount", "status")
)
