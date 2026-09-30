from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)
