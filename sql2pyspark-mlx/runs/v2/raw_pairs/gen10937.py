from pyspark.sql import functions as F

result = (
    df_payments
    .cube("method", F.to_date("paid_at").alias("paid_day"))
    .agg(F.avg("amount").alias("avg_amount"))
)
