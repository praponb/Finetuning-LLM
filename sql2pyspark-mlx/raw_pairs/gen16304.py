from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .groupBy("merchant")
    .pivot("txn_type", ["CREDIT", "FEE", "DEBIT", "REFUND"])
    .agg(F.max("amount"))
)
