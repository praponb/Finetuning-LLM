from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([6, 19]))
    .groupBy(
        F.when(F.col("txn_type").isin(["CREDIT", "DEBIT"]), "tier_1").otherwise("other").alias("txn_type_group"),
    )
    .agg(F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"))
)
