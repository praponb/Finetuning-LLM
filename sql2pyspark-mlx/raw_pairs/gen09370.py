from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2025-02-13")
    .groupBy(
        "method",
        "status",
        F.when(F.col("amount") >= 1480, "high").when(F.col("amount") >= 300, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.count("amount").alias("amount_count"),
    )
)
