from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type") != "LOAN")
    .groupBy(F.col("a.account_type"))
    .agg(F.min(F.col("t.amount")).alias("min_amount"))
)
