from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2025-01-01")
    .groupBy("txn_type")
    .pivot("merchant", ["Amazon", "Lazada"])
    .agg(F.max("amount"))
)
