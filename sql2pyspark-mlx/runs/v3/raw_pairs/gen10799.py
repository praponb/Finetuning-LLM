from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"), "merchant", "txn_type")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)
