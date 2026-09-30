from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .groupBy("status", F.quarter("paid_at").alias("paid_quarter"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") < 55)
    .orderBy(F.col("stddev_amount").desc(), "status", "paid_quarter")
    .limit(10)
)
