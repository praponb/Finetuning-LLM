from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-08-01")
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .pivot("merchant", ["Grab", "Starbucks", "Shell"])
    .agg(F.avg("amount"))
    .orderBy("txn_quarter")
)
