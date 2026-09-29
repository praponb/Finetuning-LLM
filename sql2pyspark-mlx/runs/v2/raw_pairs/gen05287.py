from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .pivot("txn_type", ["REFUND", "DEBIT"])
    .agg(F.avg("amount"))
    .orderBy("merchant")
)
