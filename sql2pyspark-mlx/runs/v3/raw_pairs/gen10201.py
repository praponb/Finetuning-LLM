from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.sum("amount").alias("total_amount"),
    )
)
