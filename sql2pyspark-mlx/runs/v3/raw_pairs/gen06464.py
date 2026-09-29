from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
    )
)
