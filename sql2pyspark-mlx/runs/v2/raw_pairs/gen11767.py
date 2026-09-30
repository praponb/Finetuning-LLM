from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Starbucks", "7-Eleven"]))
    .groupBy("txn_type")
    .pivot("merchant", ["7-Eleven", "Lazada"])
    .agg(F.count("amount"))
)
