from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("paid_at") < "2025-05-01"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy(F.col("stddev_amount").desc(), F.col("total_amount").desc())
)
