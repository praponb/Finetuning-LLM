from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "merchant",
        F.when(F.col("merchant").isin(["7-Eleven", "Starbucks"]), "core").otherwise("other").alias("merchant_group"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
    .filter(F.col("7_eleven_amount") >= 35)
)
