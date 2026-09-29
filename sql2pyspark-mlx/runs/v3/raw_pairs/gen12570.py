from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.min("amount").alias("min_amount"),
    )
)
