from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("amount_range") >= 31)
)
