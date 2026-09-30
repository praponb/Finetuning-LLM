from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"), "merchant")
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
        F.count("*").alias("num_transactions"),
    )
    .filter(F.col("fee_amount") >= 23)
    .orderBy("num_transactions")
)
