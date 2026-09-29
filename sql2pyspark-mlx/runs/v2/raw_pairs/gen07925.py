from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.countDistinct("amount").alias("unique_methods"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)
