from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("is_flagged")
    .agg(F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"))
    .filter(F.col("credit_count") >= 9)
    .orderBy("credit_count", "is_flagged")
    .limit(20)
)
