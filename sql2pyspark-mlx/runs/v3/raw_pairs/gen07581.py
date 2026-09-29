from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_time") >= "2023-11-01").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .select(F.col("a.id"), F.col("a.customer_id"))
)
