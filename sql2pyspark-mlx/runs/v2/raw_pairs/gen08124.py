from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & F.col("method").isin(["cash", "card", "paypal"]))
    .agg(F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"))
    .orderBy(F.col("promptpay_count").desc())
)
