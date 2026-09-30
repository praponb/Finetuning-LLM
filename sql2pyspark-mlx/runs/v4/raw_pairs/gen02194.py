from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2024-10-20") & (F.col("order_id") == 15))
    .groupBy("status")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
