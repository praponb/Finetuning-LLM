from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "card", "paypal"]))
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .pivot("status", ["SUCCESS", "FAILED", "REFUNDED"])
    .agg(F.avg("amount"))
    .orderBy("paid_day")
)
