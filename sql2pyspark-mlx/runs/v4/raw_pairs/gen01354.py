from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("amount").alias("amount_count"),
    )
)
