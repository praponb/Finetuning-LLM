from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .rollup(
        F.col("a.account_type").alias("account_type"),
        F.col("e.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("e.amount")), 2).alias("total_amount"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("total_amount").desc_nulls_last())
)
