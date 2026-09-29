from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("account_id").isin([7, 14, 26, 28])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.balance") > 77000)
    .select(F.col("a.id"), F.col("a.account_type"), F.col("a.status"))
    .orderBy("status")
)
