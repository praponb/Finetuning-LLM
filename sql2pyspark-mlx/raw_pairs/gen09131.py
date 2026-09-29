from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
    .filter(F.col("refunded_count") > 1)
)
