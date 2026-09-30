from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .groupBy("account_id")
    .agg(
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .filter(F.col("tba.pct_credit") > 43)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("tba.refund_count"),
        F.col("tba.pct_credit"),
    )
    .orderBy(F.col("id").desc())
)
