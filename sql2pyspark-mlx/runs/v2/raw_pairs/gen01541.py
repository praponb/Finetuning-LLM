from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1350, "high").when(F.col("amount") >= 450, "mid").otherwise("low").alias("amount_bucket"),
        "status",
        "method",
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)
