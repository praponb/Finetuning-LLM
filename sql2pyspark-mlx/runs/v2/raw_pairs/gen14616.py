from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "card", "paypal"]) & (F.col("amount") >= 600))
    .withColumn("paid_at_date", F.to_date("paid_at"))
)

summary = (
    valid_payments
    .groupBy(
        "method",
        F.when(F.col("amount") >= 1260, "high").when(F.col("amount") >= 380, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    summary
    .filter(F.col("max_amount") < 840)
    .select("method", "amount_bucket", "max_amount")
)
