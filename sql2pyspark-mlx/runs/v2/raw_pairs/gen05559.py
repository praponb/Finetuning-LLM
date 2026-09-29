from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "method")
    .agg(F.min("paid_at").alias("min_paid_at"))
)
