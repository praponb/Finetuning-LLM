from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.customer_id").isNotNull())
    .groupBy(F.col("a.account_type"))
    .agg(F.min(F.col("t.txn_time")).alias("first_txn_time"))
    .orderBy("account_type")
)
