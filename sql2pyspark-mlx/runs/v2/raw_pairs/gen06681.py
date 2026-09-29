from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("txn_type")
    .pivot("merchant", ["Starbucks", "Amazon", "Lazada"])
    .agg(F.sum("amount"))
)
