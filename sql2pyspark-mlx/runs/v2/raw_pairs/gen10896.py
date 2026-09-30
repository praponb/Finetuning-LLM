from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
    )
    .filter(F.col("fee_count") < 9)
)
