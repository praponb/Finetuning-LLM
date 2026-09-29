from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter((F.col("txn_time") >= "2025-06-11") | (F.col("amount") > 2400))
)

agg = (
    filtered_transactions
    .groupBy("account_id", "merchant")
    .agg(F.max("txn_time").alias("latest_txn_time"))
)

result = (
    agg
    .select("account_id", "merchant", "latest_txn_time")
    .orderBy("merchant")
)
