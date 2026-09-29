from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"), "method")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") > 23)
)
