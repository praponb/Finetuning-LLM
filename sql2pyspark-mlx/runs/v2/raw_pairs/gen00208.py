from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("status") == "CLOSED").alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(F.col("t.txn_time") < "2023-07-01")
    .select(F.col("t.id"), F.col("t.txn_type"))
    .orderBy("txn_type")
)
