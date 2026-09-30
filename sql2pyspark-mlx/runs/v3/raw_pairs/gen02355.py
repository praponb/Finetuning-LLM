from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "REFUNDED") & F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .groupBy("status", "order_id", F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("stddev_amount") > 61)
    .orderBy(F.col("order_id").desc())
)
