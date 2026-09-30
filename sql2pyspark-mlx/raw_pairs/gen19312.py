from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"))
)
