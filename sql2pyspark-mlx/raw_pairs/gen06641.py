from pyspark.sql import functions as F

base = (
    df_payments
    .filter(((F.col("paid_at") < "2023-06-09") & (F.col("paid_at") >= "2023-11-01")) | F.col("method").isin(["bank_transfer", "promptpay", "paypal"]))
)

summary = (
    base
    .groupBy("status", "order_id")
    .agg(F.min("paid_at").alias("first_paid_at"))
)

result = (
    summary
    .select("status", "order_id", "first_paid_at")
    .orderBy(F.col("first_paid_at").desc(), F.col("order_id").desc())
)
