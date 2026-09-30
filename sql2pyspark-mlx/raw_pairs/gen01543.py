from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .pivot("status", ["PAUSED", "EXPIRED"])
    .agg(F.avg("monthly_fee").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "customer_id",
        F.col("PAUSED_amount").alias("paused_amount"),
        F.col("PAUSED_n").alias("paused_n"),
        F.col("EXPIRED_amount").alias("expired_amount"),
        F.col("EXPIRED_n").alias("expired_n"),
    )
)
