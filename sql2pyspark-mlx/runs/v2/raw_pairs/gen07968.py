from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2024-02-01")
    .groupBy("is_flagged")
    .agg(F.min("txn_time").alias("min_txn_time"))
)
