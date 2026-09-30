from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)
