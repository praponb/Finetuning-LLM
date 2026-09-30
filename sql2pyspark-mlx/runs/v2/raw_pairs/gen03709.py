from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") >= 48)
    .orderBy("txn_month")
    .limit(1)
)
