from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([10, 12]))
    .groupBy("is_flagged", "txn_type", "merchant")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
    )
)
