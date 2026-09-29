from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("amount") > 1600)
    .withColumn("days_since_txn_time", F.datediff(F.current_date(), "txn_time"))
)

agg = (
    valid_transactions
    .groupBy("txn_type")
    .agg(F.min("txn_time").alias("first_txn_time"))
)

result = agg.select("txn_type", "first_txn_time")
