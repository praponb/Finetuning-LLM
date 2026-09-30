from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "order_id")
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
        F.count("amount").alias("amount_count"),
    )
)
