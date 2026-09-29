from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") > 75)
    .orderBy("stddev_amount", "txn_year")
    .limit(5)
)
