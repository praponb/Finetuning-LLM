from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id").isin([8, 27, 32]) & (F.col("txn_time") < "2025-03-01")) | F.col("merchant").isin(["Lazada", "Starbucks", "Amazon"]))
    .withColumn("txn_time_day", F.col("txn_time").cast("date"))
    .withColumn("merchant_txn_type_key", F.concat_ws("-", "merchant", "txn_type"))
)
