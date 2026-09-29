from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1790, "high").when(F.col("amount") >= 300, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count("*").alias("cnt"),
    )
)
