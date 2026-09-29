from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.txn_time") < "2024-10-01")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.sum(F.when(F.col("t.merchant") == "Amazon", F.col("t.amount")).otherwise(0)).alias("amazon_amount"),
        F.sum(F.when(F.col("t.txn_type") == "REFUND", F.col("t.amount")).otherwise(0)).alias("refund_amount"),
    )
    .filter(F.col("refund_amount") >= 39)
    .orderBy("amazon_amount")
)
