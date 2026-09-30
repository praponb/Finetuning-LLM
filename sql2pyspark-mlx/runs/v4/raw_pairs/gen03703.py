from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "customer_id")
    .pivot("status", ["EXPIRED", "PAUSED", "CANCELLED"])
    .agg(F.avg("monthly_fee"))
    .select(
        "plan",
        "customer_id",
        F.col("EXPIRED").alias("expired"),
        F.col("PAUSED").alias("paused"),
        F.col("CANCELLED").alias("cancelled"),
    )
)
