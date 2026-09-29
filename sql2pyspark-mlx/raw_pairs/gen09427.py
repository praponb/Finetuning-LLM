from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2023-05-07") & (F.col("account_id") == 23))
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
    )
    .orderBy("txn_type")
)
