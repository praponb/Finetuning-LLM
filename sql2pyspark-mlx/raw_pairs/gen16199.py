from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "left")
    .groupingSets(
        [["account_type", "status", "txn_type"], ["account_type", "status"], ["account_type"]],
        F.col("a.account_type").alias("account_type"),
        F.col("a.status").alias("status"),
        F.col("src.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("src.amount")).alias("avg_amount"),
        F.grouping_id().alias("gid"),
    )
)
