from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-09-01")
    .groupBy("order_id")
    .pivot("method", ["paypal", "card"])
    .agg(F.avg("amount"))
    .orderBy("order_id")
)
