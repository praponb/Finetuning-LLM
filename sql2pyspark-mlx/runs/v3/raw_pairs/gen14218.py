from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("status") == "FROZEN").alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(F.hour(F.col("t.txn_time")).between(9, 16))
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.account_id"),
        F.col("t.merchant"),
    )
    .orderBy("id")
)
