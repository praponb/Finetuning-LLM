from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-06-30 08:00:00")))
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.length("merchant").alias("merchant_length"),
    )
)

result = (
    cleaned
    .withColumn("merchant_length_tier", F.when(F.col("merchant_length") >= 66, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["merchant_length_tier", "merchant"], ["merchant_length_tier"], []],
        "merchant_length_tier",
        "merchant",
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_rows"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
        F.round(F.avg("merchant_length"), 2).alias("avg_merchant_length"),
    )
    .filter(F.col("refund_rows") > 1)
    .orderBy(F.col("refund_rows").asc_nulls_first())
)
