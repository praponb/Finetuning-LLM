from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(6, 11) & (F.col("amount") > 2400))
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .orderBy("txn_month", F.col("stddev_amount").desc())
)
