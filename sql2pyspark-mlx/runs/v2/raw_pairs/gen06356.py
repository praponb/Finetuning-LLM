from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 7) | F.col("txn_type").isin(["REFUND", "CREDIT", "FEE"]))
    .groupBy("txn_type")
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
    )
)
