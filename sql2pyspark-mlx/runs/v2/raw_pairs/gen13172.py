from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.to_date("txn_time") < "2023-04-01").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.balance") > 73000)
    .select(F.col("a.id"), F.col("a.customer_id"))
)
