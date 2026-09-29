from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter((F.col("amount") <= 1280) & (F.col("order_id") == 25))
    .withColumn("paid_at_hour", F.hour("paid_at"))
)

summary = (
    recent_payments
    .groupBy("method", "status")
    .agg(F.min("paid_at").alias("min_paid_at"))
)

result = (
    summary
    .select("method", "status", "min_paid_at")
    .orderBy(F.col("method").desc())
)
