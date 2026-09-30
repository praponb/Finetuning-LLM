from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "bank_transfer", "paypal"]) | (F.to_date("paid_at") >= "2023-12-01"))
    .groupBy(
        F.when(F.col("amount") >= 1550, "high").when(F.col("amount") >= 650, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.min("amount").alias("min_amount"),
        F.avg("amount").alias("avg_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
)
