from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter((F.col("amount") >= 1050) & F.col("method").isin(["paypal", "bank_transfer", "promptpay"]))
)

agg = (
    recent_payments
    .groupBy("status")
    .agg(F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"))
)

result = (
    agg
    .filter(F.col("promptpay_count") >= 4)
    .select("status", "promptpay_count")
)
