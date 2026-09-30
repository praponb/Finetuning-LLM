from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.refund_amount") >= 39)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("t.refund_amount"),
        F.col("t.avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc(), "id")
    .limit(1)
)
