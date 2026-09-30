from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([2, 15, 28]) | (F.col("paid_at") < "2024-07-01"))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"), "order_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") < 30)
    .orderBy(F.col("stddev_amount").desc())
)
