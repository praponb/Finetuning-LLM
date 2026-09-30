from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") >= "2023-07-01") & F.col("method").isin(["card", "paypal"]) & (F.col("amount") > 1190))
    .select("method", "order_id", "amount", F.to_date("paid_at").alias("paid_at_date"))
)
