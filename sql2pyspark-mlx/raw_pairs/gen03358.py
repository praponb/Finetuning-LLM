from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-03-12")
    .groupBy("txn_type")
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
    )
    .filter(F.col("fee_amount") < 31)
    .select("txn_type", "fee_amount")
)
