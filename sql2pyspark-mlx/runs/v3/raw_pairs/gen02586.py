from pyspark.sql import functions as F

valid_payments = df_payments.filter(F.col("method") != "paypal")

status_summary = (
    valid_payments
    .groupBy("status", "method")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    status_summary
    .select("status", "stddev_amount")
    .orderBy("status")
)
