from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.customer_id") == 11)
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.max(F.col("t.amount")).alias("max_amount"),
        F.sum(F.when(F.col("t.txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
)
