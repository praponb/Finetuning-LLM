from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id").isin([6, 18]) & (F.col("amount") <= 3400)) | (F.col("txn_type") == "REFUND"))
    .select("id", "is_flagged", "txn_type")
)
