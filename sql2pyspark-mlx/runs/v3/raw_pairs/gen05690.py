from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(~F.col("t.is_flagged"))
    .groupBy(F.col("a.customer_id"))
    .agg(F.sum(F.col("t.amount")).alias("total_amount"))
)
