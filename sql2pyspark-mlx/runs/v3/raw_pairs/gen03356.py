from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"))
    .filter(F.col("refund_count") > 3)
    .select("merchant", "refund_count")
    .orderBy(F.col("refund_count").desc(), "merchant")
    .limit(10)
)
