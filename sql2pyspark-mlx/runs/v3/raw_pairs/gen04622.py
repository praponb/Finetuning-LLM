from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([20, 32]) & (F.col("order_id") == 36))
    .groupBy("status")
    .agg(F.min("paid_at").alias("min_paid_at"))
)
