from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1380, "high").when(F.col("amount") >= 550, "mid").otherwise("low").alias("amount_bucket"),
        "status",
        "method",
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
    .filter(F.col("max_amount") < 910)
    .orderBy(F.col("max_amount").desc())
)
