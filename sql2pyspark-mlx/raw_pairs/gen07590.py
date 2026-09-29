from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Amazon", "Shell", "Grab", "7-Eleven"])
    .agg(F.sum("amount"))
    .orderBy("txn_type")
)
