from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1240, 1260))
    .groupBy("method", F.quarter("paid_at").alias("paid_quarter"))
    .agg(F.sum("amount").alias("total_amount"))
)
