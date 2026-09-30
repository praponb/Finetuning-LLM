from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 2) & (F.col("amount") >= 870))
    .groupBy(
        F.when(F.col("amount") >= 1650, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("amount_range") < 76)
)
