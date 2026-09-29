from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("is_flagged") & F.col("txn_type").isin(["CREDIT", "REFUND"])) | F.col("is_flagged"))
    .select("txn_type")
)
