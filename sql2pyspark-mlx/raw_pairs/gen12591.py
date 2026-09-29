from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") < "2024-10-01") | (F.col("amount") < 3700))
    .groupBy(F.month("txn_time").alias("txn_month"), "merchant")
    .agg(F.max("txn_time").alias("latest_txn_time"))
    .orderBy(F.col("txn_month").desc(), "merchant")
    .limit(1)
)
