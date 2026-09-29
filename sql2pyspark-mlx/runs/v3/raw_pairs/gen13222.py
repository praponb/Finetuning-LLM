from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.status").isin(["CLOSED", "OPEN"]))
    .select(F.col("a.status"), F.col("t.txn_time"), F.col("t.is_flagged"))
    .orderBy("txn_time")
)
