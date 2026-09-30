from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.account_type").isin(["LOAN", "SAVINGS"]))
    .groupBy(F.col("a.status"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
        F.count(F.when(F.col("t.txn_type") == "DEBIT", 1)).alias("debit_count"),
    )
    .orderBy(F.col("status").desc(), "pct_shell")
    .limit(20)
)
