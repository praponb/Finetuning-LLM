from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") != "Grab") & F.col("is_flagged"))
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"), "txn_type")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("amount").alias("min_amount"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
    )
    .filter(F.col("refund_amount") < 64)
    .drop("refund_amount")
)
