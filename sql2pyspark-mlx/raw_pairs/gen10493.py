from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-09-05 03:00:00").cast("timestamp"))
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("paid_at_date", F.to_date("paid_at"))
)

result = (
    filtered
    .groupBy("order_id")
    .pivot("method", ["paypal", "card"])
    .agg(F.max("amount"))
)
