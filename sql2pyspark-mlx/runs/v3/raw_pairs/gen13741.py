from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(7, 11))
    .groupBy("txn_type")
    .pivot("merchant", ["Amazon", "Lazada", "Grab"])
    .agg(F.avg("amount"))
    .orderBy("txn_type")
)
