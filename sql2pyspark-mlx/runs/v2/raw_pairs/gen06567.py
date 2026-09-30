from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.date_format("paid_at", "yyyy-MM").alias("paid_month"),
        F.year("paid_at").alias("paid_year"),
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum("amount").alias("total_amount"),
    )
)
