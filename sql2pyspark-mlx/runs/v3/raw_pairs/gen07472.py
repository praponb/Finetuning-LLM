from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.quarter("paid_at").alias("paid_quarter"), "order_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") > 57)
    .select("paid_quarter", "order_id", "stddev_amount")
    .orderBy(F.col("order_id").desc(), F.col("paid_quarter").desc())
)
