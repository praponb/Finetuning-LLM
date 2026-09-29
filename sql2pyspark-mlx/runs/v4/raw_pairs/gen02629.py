from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.amount") < 1500)
    .groupBy(F.col("a.status"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.round(((100 * F.sum(F.when(F.col("t.txn_type") == "DEBIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_debit"),
        F.avg(F.col("t.amount")).alias("avg_amount"),
    )
    .orderBy(F.col("pct_debit").desc(), "status")
)
