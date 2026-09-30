from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.when(F.col("amount") >= 1530, "high").when(F.col("amount") >= 590, "mid").otherwise("low").alias("amount_bucket"))
    .pivot("method", ["paypal", "promptpay", "cash", "card"])
    .agg(F.avg("amount"))
    .orderBy("amount_bucket")
)
