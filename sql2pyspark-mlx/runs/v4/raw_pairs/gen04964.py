from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("balance") > 26000).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .filter(F.to_date(F.col("t.txn_time")) < "2024-08-01")
    .select(F.col("t.id"), F.col("t.account_id"), F.col("t.merchant"))
)
