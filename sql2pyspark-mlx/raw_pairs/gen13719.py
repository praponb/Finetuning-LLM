from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([17, 19, 33]))
    .groupBy("account_id")
    .agg(F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"))
    .filter(F.col("fee_count") < 77)
    .orderBy(F.col("account_id").desc())
)
