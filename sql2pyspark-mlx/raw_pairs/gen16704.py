from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .select("*", F.round("amount", 0).alias("amount_rounded"))
)

result = (
    base
    .groupBy("order_id")
    .pivot("method", ["promptpay", "paypal", "bank_transfer"])
    .agg(F.avg("amount"))
)
