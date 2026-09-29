from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "method",
        "status",
        F.when(F.col("amount") >= 1350, "high").when(F.col("amount") >= 650, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy("method")
)
