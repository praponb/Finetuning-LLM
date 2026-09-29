from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([21, 27, 38]) | (F.col("amount") <= 3500))
    .groupBy("is_flagged", "merchant")
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc(), "is_flagged", "merchant")
    .limit(5)
)
