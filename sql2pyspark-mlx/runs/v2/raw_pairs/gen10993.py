from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") >= "2024-06-01") | (F.col("paid_at") >= "2023-05-13"))
    .groupBy("method", F.month("paid_at").alias("paid_month"))
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy("paid_month", "stddev_amount")
)
