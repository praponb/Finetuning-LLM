from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["card", "bank_transfer"]))
    .groupBy(
        "status",
        F.when(F.col("amount") >= 1280, "high").when(F.col("amount") >= 470, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("amount").alias("amount_count"),
        F.max("paid_at").alias("latest_paid_at"),
    )
    .orderBy(F.col("amount_bucket").desc())
)
