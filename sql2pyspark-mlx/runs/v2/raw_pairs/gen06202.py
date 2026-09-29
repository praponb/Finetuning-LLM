from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNotNull() & ((F.col("amount") >= 530) | (F.col("amount") > 620)))
    .groupBy(
        F.when(F.col("amount") >= 1250, "high").when(F.col("amount") >= 720, "mid").otherwise("low").alias("amount_bucket"),
        "method",
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
