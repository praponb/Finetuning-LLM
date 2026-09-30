from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["7-Eleven", "Shell"])
    .agg(F.count("amount"))
    .orderBy("txn_type")
)
