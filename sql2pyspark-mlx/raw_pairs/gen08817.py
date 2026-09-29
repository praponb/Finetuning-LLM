from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") > 30)
)
