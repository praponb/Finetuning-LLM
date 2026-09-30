from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Lazada")
    .select("id", "txn_type", "merchant", F.lower("merchant").alias("merchant_lower"))
)
