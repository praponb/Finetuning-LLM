from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("stddev_amount") > 62)
    .select("status", "first_paid_at", "stddev_amount")
)
