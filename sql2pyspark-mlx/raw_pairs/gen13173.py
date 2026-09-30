from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["card", "promptpay"]))
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") < 5100)
    .orderBy(F.col("stddev_amount").desc())
)
