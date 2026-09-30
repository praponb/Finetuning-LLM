from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy(F.year("txn_time").alias("txn_year"))
    .pivot("txn_type", ["CREDIT", "REFUND", "DEBIT", "FEE"])
    .agg(F.count("amount"))
    .orderBy("txn_year")
)
