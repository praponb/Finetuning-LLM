from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Amazon", "Shell"])
    .agg(F.avg("amount"))
)
