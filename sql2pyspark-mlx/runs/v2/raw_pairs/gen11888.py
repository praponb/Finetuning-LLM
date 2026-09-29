from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([8, 30, 31, 34]))
    .withColumn("paid_at_period", F.date_trunc("WEEK", "paid_at"))
)
