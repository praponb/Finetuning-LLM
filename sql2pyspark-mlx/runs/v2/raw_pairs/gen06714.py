from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_type").isin(["FEE", "CREDIT", "REFUND"])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.customer_id") == 25)
    .select(F.col("a.id"), F.col("a.status"))
)
