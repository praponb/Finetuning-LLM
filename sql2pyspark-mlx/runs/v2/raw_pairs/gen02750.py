from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.max(F.col("t.amount")).alias("max_amount"),
        F.sum(F.when(F.col("t.txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
    .orderBy(F.col("max_amount").desc(), "account_type")
    .limit(50)
)
