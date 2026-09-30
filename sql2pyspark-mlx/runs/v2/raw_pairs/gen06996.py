from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isin([1, 3, 19]) & F.col("method").isin(["promptpay", "card", "bank_transfer"]))
)

payments_stats = (
    base
    .groupBy("order_id", F.to_date("paid_at").alias("paid_day"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    payments_stats
    .select("order_id", "paid_day")
    .orderBy("order_id", F.col("paid_day").desc())
)
