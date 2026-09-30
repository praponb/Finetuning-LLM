from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .pivot("txn_type", ["REFUND", "FEE"])
    .agg(F.sum("amount"))
    .orderBy("is_flagged")
)
