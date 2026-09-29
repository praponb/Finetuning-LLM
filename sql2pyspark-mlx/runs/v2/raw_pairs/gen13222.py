from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "promptpay", "paypal"]))
    .groupBy("status")
    .agg(F.max("paid_at").alias("last_paid_at"))
)
