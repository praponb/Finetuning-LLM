from pyspark.sql import functions as F

base = df_payments.filter(F.col("amount") < 1040)

payments_stats = (
    base
    .groupBy(
        F.when(F.col("amount") >= 1740, "high").when(F.col("amount") >= 790, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0)).alias("promptpay_count"),
    )
)

result = (
    payments_stats
    .select("amount_bucket", "promptpay_count")
    .orderBy("promptpay_count", F.col("amount_bucket").desc())
)
