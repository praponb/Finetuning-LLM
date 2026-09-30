from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "txn_time", "account_id", F.lower("merchant").alias("merchant_lower"))
    .orderBy("merchant_lower")
)
