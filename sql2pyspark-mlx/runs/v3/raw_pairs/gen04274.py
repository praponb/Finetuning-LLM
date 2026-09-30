from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1290, "high").when(F.col("amount") >= 630, "mid").otherwise("low").alias("amount_bucket"),
        F.month("paid_at").alias("paid_month"),
        "status",
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 42)
    .orderBy("amount_bucket")
)
