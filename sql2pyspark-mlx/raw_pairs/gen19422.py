from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.year("txn_time").alias("txn_year"), "merchant")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
    )
    .orderBy(F.col("min_amount").desc())
)
