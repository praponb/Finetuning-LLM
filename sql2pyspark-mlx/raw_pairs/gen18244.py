from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "inner")
    .cube(
        F.col("d.account_type").alias("account_type"),
        F.col("a.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.amount")).alias("max_amount"),
    )
    .orderBy(F.col("rows_total").asc_nulls_last())
)
