from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Grab", "7-Eleven", "Starbucks", "Lazada"])
    .agg(F.count("amount"))
)
