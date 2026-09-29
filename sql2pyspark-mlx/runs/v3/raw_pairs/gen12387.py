from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.customer_id"), F.col("t.txn_type"), F.col("t.txn_time"))
    .orderBy("txn_type", F.col("txn_time").desc())
)
