from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") != "promptpay") & (F.col("status") == "FAILED"))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"), "status")
    .agg(F.min("amount").alias("min_amount"))
)
