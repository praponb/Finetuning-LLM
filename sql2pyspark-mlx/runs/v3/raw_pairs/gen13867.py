from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 790)
    .agg(F.max("paid_at").alias("latest_paid_at"))
)
