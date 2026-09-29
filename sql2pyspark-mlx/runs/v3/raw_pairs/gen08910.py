from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Starbucks", "7-Eleven", "Shell", "Lazada"])
    .agg(F.count("amount"))
)
