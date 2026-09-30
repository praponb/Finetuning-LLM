from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Lazada", "Starbucks", "Amazon"])
    .agg(F.avg("amount"))
    .orderBy("txn_type")
)
