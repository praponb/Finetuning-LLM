from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") < "2025-03-01")
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(F.min("txn_time").alias("min_txn_time"))
)
