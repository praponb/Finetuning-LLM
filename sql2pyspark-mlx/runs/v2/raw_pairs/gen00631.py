from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-05-01")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
