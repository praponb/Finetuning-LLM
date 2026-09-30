from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("account_type") != "CREDIT").alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .filter(F.col("t.merchant").isNull())
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.account_id"),
        F.col("t.txn_time"),
    )
)
