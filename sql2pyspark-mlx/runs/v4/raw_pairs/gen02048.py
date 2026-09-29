from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Starbucks", "Amazon", "Lazada", "Shell"])
    .agg(F.avg("amount"))
)
