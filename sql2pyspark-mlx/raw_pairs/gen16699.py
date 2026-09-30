from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_type") == "REFUND").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.balance") > 30000)
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("a.account_type"))
    .orderBy("account_type")
)
