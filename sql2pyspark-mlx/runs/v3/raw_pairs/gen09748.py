from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1400, 3700))
    .groupBy("txn_type")
    .pivot("merchant", ["Lazada", "7-Eleven"])
    .agg(F.avg("amount"))
)
