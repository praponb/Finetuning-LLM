from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") < "2025-10-01") & (F.col("merchant") == "7-Eleven"))
    .select("id", "merchant", F.length("txn_type").alias("txn_type_length"))
)
