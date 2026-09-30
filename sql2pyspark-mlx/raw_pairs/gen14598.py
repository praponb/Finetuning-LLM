from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-02-01")
    .groupBy("txn_type")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") > 2900)
)
