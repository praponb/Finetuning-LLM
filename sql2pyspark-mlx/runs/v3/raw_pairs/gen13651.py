from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(F.col("a.status"))
    .agg(
        F.countDistinct(F.col("t.amount")).alias("unique_txn_types"),
        F.min(F.col("t.amount")).alias("min_amount"),
    )
    .orderBy("unique_txn_types", "status")
    .limit(1)
)
