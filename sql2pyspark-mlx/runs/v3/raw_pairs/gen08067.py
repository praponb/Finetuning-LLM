from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2025-09-01") & F.col("method").isin(["promptpay", "paypal"]))
    .select("id", "method", F.col("amount").cast("int").alias("amount_int"))
)
