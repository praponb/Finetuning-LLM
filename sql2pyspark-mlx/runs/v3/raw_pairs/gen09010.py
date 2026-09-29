from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant").isin(["Shell", "Starbucks"]) & (F.col("amount") >= 2400)) | (F.col("merchant") == "Lazada"))
    .select("id", "txn_type")
)
