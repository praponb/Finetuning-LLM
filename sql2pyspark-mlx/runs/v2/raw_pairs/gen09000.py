from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter((F.col("status") == "REFUNDED") & F.hour("paid_at").between(6, 13))
)

summary = (
    recent_payments
    .groupBy(
        F.when(F.col("amount") >= 1590, "high").when(F.col("amount") >= 780, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.max("paid_at").alias("last_paid_at"))
)

result = summary.select("amount_bucket", "last_paid_at")
