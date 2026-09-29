from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .filter(F.col("A.pct_refund") >= 54)
    .select(F.col("a.customer_id"), F.col("A.total_amount"), F.col("A.pct_refund"))
)
