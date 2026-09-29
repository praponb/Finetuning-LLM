from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") < 1)
)
