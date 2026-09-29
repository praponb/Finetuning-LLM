from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["paypal", "promptpay", "bank_transfer"]))
    .groupBy("order_id")
    .pivot("status", ["REFUNDED", "FAILED"])
    .agg(F.max("amount"))
    .orderBy("order_id")
)
