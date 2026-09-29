from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("A.pct_refund"),
    )
)
