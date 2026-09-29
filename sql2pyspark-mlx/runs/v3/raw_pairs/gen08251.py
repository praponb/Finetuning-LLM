from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["promptpay", "card", "paypal"]) | F.col("order_id").isNotNull())
    .groupBy("order_id", "method")
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") >= 840)
)
