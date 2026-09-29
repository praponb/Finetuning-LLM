from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("paid_at") < "2024-01-01") & F.col("amount").between(1040, 1180))
    .withColumn("status_prefix", F.substring("status", 1, 4))
)

order_id_summary = (
    recent_payments
    .groupBy("order_id", F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    order_id_summary
    .filter(F.col("max_amount") >= 1090)
    .select("order_id", "paid_month", "pct_refunded", "max_amount")
)
