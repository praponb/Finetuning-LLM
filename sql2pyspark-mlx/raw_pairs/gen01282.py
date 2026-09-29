from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_time") < "2025-08-16").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .select(F.col("a.id"), F.col("a.status"))
    .orderBy("status", "id")
)
