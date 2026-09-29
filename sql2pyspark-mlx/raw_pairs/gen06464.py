from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 990)
    .groupBy(
        F.when(F.col("amount") >= 1560, "high").when(F.col("amount") >= 240, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.min("paid_at").alias("first_paid_at"))
    .orderBy("first_paid_at")
)
