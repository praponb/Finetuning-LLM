from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(F.sum("amount").alias("total_amount"))
)
