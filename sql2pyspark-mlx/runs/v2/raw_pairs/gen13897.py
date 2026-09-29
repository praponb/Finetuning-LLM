from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.col("a.status"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
        F.min(F.col("t.amount")).alias("min_amount"),
    )
)
