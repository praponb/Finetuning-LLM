from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .filter(F.col("t.merchant") == "Shell")
    .groupBy(F.col("a.account_type"), F.col("a.status"))
    .agg(
        F.min(F.col("t.txn_time")).alias("min_txn_time"),
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
    )
    .orderBy("status", F.col("avg_amount").desc())
)
