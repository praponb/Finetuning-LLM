from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["status", "txn_type"], ["status"], []],
        F.col("p.status").alias("status"),
        F.col("s.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("s.amount")), 2).alias("avg_amount"),
        F.grouping_id().alias("level_id"),
    )
)
