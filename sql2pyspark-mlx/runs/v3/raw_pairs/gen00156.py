from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "DEBIT")
    .groupBy("txn_type")
    .pivot("merchant", ["Starbucks", "Shell"])
    .agg(F.avg("amount"))
)
