from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("txn_type") != "DEBIT")
    .groupBy("account_id")
    .agg(F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("T"), F.col("a.id") == F.col("T.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("T.fee_count"),
    )
    .orderBy("customer_id")
)
