from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "REFUND")
    .groupBy("txn_type")
    .pivot("merchant", ["Lazada", "Shell", "7-Eleven"])
    .agg(F.max("amount"))
    .orderBy("txn_type")
)
