from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.merchant").isin(["Lazada", "7-Eleven"]))
    .groupBy(F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.sum(F.when(F.col("t.txn_type") == "FEE", F.col("t.amount")).otherwise(0)).alias("fee_amount"),
    )
    .orderBy("account_type")
)
