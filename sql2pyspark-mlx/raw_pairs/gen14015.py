from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("merchant").isNull())
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("as.amount_range"),
        F.col("as.pct_refund"),
    )
    .orderBy("status", "id")
    .limit(10)
)
