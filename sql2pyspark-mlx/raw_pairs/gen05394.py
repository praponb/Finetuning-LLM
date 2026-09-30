from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("order_id").isin([5, 18]))

status_summary = (
    filtered_payments
    .groupBy("status", "method")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = status_summary.filter(F.col("avg_amount") > 1260)
