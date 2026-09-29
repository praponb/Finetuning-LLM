from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") >= 34)
)
