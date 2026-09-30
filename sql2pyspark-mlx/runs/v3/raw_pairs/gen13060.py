from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.balance") > 69000)
    .select(F.col("a.status"), F.col("t.merchant"), F.col("t.txn_type"))
)
