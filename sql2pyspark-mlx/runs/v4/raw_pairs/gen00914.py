from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "7-Eleven")
    .groupBy("is_flagged")
    .pivot("txn_type", ["DEBIT", "REFUND", "CREDIT"])
    .agg(F.max("amount"))
)
