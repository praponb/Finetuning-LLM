from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_type").isin(["CREDIT", "FEE", "DEBIT"])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .select(F.col("a.id"), F.col("a.account_type"))
    .orderBy("account_type")
)
