from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter(F.col("txn_time") > F.to_timestamp(F.lit("2023-08-20 07:00:00")))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("UNKNOWN")))
)

result = (
    filtered
    .groupBy("merchant")
    .pivot("txn_type", ["CREDIT", "REFUND"])
    .agg(F.avg("amount"))
)
