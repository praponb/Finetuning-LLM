from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "promptpay", "paypal"]))
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("first_paid_at").desc())
)
