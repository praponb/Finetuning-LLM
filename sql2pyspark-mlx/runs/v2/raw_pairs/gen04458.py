from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("amount") >= 3000)
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .select(
        F.col("a.balance"),
        F.col("a.status"),
        F.col("A.total_amount"),
        F.col("A.refund_amount"),
    )
    .orderBy("total_amount")
)
