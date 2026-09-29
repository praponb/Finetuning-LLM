from pyspark.sql import functions as F

result = (
    df_payments
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("paid_at").alias("latest_paid_at"),
    )
)
