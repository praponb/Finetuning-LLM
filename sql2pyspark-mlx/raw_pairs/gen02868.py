from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "promptpay") & F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .groupBy("status")
    .agg(
        F.count("amount").alias("amount_count"),
        F.avg("amount").alias("avg_amount"),
    )
)
