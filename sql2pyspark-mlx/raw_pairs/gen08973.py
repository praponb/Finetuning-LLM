from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Grab", "Shell"]))
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("t.credit_count"),
        F.col("t.pct_shell"),
    )
    .orderBy(F.col("pct_shell").desc())
)
