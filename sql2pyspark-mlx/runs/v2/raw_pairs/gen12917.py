from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("customer_id").isin([3, 17, 29])).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .select(F.col("t.id"), F.col("t.txn_time"))
)
