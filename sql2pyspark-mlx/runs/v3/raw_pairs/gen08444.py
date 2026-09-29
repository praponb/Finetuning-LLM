from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("paid_at") < "2025-02-01"))
    .groupBy("order_id")
    .agg(F.max("paid_at").alias("last_paid_at"))
)
