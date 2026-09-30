from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount").between(1300, 2000) & (F.col("amount") <= 3800)) | ~F.col("is_flagged"))
    .select("txn_time", "amount", "txn_type")
)
