from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Grab", "Starbucks"]))
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.count("amount").alias("amount_count"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
    )
)
