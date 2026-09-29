from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("amount") > 1600)
    .withColumn("days_since_txn_time", F.datediff(F.current_date(), "txn_time"))
)

agg = (
    valid_transactions
    .groupBy("txn_type")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = agg.filter(F.col("amount_range") > 73)
