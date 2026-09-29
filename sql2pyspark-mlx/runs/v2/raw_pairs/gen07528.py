from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.status") == "OPEN")
    .groupBy(F.quarter(F.col("a.opened_date")).alias("opened_quarter"))
    .agg(
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
        F.max(F.col("t.txn_time")).alias("latest_txn_time"),
    )
    .orderBy("avg_amount", "latest_txn_time")
)
