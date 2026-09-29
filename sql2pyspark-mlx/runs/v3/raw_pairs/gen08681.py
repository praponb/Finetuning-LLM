from pyspark.sql import functions as F

result = (
    df_payments
    .rollup("method", "status")
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.min("amount").alias("min_amount"),
    )
)
