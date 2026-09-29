from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.hour("paid_at").between(6, 11) & F.col("order_id").isNotNull() & (F.col("order_id") == 35))
)

status_summary = (
    base
    .groupBy("status")
    .agg(F.count("*").alias("payment_count"))
)

result = (
    status_summary
    .select("status", "payment_count")
    .orderBy("status", F.col("payment_count").desc())
)
