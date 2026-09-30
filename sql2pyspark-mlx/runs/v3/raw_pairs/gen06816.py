from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.month("paid_at").alias("paid_month"))
    .pivot("status", ["REFUNDED", "FAILED", "SUCCESS"])
    .agg(F.max("amount"))
    .orderBy("paid_month")
)
