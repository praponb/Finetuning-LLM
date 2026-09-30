from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.hour("paid_at").between(12, 17) & F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]) & (F.col("paid_at") >= "2023-04-01"))
)

payments_stats = (
    valid_payments
    .groupBy("method")
    .agg(F.max("paid_at").alias("last_paid_at"))
)

result = (
    payments_stats
    .select("method", "last_paid_at")
    .orderBy(F.col("last_paid_at").desc())
)
