from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-02-16")
    .groupBy("txn_type")
    .pivot("merchant", ["Grab", "7-Eleven", "Starbucks", "Amazon"])
    .agg(F.avg("amount"))
    .orderBy("txn_type")
)
