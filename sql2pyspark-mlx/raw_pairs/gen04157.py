from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("dim"), F.col("src.account_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["status", "txn_type"], ["status"]],
        F.col("dim.status").alias("status"),
        F.col("src.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.amount")).alias("min_amount"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("txn_type").desc_nulls_first())
)
