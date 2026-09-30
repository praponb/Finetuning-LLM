from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.when(F.col("amount") >= 1660, "high").when(F.col("amount") >= 570, "mid").otherwise("low").alias("amount_bucket"))
    .pivot("status", ["SUCCESS", "REFUNDED"])
    .agg(F.avg("amount"))
)
