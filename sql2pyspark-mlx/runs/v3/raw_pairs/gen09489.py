from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([10, 20, 30]))
    .groupBy(
        F.when(F.col("amount") >= 1210, "high").when(F.col("amount") >= 710, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") > 790)
)
