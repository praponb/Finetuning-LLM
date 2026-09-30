from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("balance") >= 61000).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(F.col("t.account_id").isin([2, 14, 20]))
    .select(F.col("t.id"), F.col("t.is_flagged"))
)
