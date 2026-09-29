from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Lazada", "Shell", "Grab", "7-Eleven"])
    .agg(F.count("amount"))
    .orderBy("txn_type")
)
