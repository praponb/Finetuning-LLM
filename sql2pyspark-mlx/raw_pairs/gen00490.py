from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("amount") >= 1500) & F.col("account_id").isin([26, 29])) | (F.col("amount") >= 4000))
    .select("id", "merchant", "txn_type", "account_id")
)
