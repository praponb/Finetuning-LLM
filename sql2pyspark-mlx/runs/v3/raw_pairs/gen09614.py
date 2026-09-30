from pyspark.sql import functions as F

recent_payments = df_payments.filter(F.col("status") == "FAILED")

agg = (
    recent_payments
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    agg
    .select("paid_hour", "stddev_amount")
    .orderBy(F.col("stddev_amount").desc(), "paid_hour")
    .limit(10)
)
