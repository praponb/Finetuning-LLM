from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-06-06")
    .groupBy("merchant")
    .pivot("txn_type", ["DEBIT", "CREDIT", "REFUND", "FEE"])
    .agg(F.avg("amount"))
    .orderBy("merchant")
)
