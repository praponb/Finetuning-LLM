from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0)).alias("refunded_count"),
    )
    .orderBy("method")
)
