from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("amount").between(2000, 2400))
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
        F.count("*").alias("transaction_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .select(
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("A.debit_count"),
        F.col("A.transaction_count"),
    )
    .orderBy("debit_count")
)
