from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(~F.col("t.merchant").isin(["Amazon", "Shell"]))
    .groupBy(F.col("a.status"))
    .agg(
        F.count(F.col("t.amount")).alias("amount_count"),
        F.max(F.col("t.txn_time")).alias("latest_txn_time"),
    )
    .orderBy("status")
    .limit(1)
)
