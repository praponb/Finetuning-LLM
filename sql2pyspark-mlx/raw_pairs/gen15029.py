from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 1330) & ((F.col("status") == "SUCCESS") | (F.col("method") == "paypal")))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"), "status")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)
