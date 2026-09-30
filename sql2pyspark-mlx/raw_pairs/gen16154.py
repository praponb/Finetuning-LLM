from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & F.hour("paid_at").between(8, 16) & (F.col("amount") <= 1120))
    .groupBy("method")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
)
