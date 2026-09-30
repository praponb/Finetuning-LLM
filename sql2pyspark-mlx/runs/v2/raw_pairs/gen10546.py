from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("amount").between(1200, 3600))
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("as.pct_refund"),
        F.col("as.amount_count"),
    )
    .orderBy("pct_refund")
)
