from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .orderBy(F.col("paid_month").desc())
)
