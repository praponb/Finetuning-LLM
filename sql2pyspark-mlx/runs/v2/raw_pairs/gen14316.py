from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Lazada"]) & F.col("merchant").isNotNull())
)

merchant_summary = (
    filtered_transactions
    .groupBy("merchant")
    .agg(F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"))
)

result = merchant_summary.select("merchant", "refund_count")
