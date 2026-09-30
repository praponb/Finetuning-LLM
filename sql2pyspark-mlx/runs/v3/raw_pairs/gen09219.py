from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(570, 1250) & F.col("order_id").isin([5, 7, 18]))
    .groupBy("status")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
    )
)
