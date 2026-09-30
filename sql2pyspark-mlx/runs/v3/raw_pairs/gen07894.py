from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.to_date("txn_time") < "2025-10-01").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .select(F.col("a.id"), F.col("a.opened_date"))
    .orderBy("opened_date", F.col("id").desc())
    .limit(5)
)
