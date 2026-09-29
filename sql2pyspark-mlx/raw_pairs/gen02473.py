from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Amazon", "Shell"]) & F.col("is_flagged") & F.col("account_id").isin([13, 14, 31]))
    .groupBy(
        "is_flagged",
        F.when(F.col("amount") >= 3800, "high").when(F.col("amount") >= 1500, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.sum("amount").alias("total_amount"),
    )
)
