from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .groupBy("account_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.refund_amount") > 53)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("t.min_amount"),
        F.col("t.refund_amount"),
    )
)
