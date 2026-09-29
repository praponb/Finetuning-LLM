from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
    )
    .orderBy(F.col("max_amount").desc())
)
