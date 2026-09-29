from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("merchant").isin(["Starbucks", "Amazon", "7-Eleven"]) & (F.col("txn_time") >= "2025-09-01"))
    .select("id", "account_id", "is_flagged", "merchant")
)
