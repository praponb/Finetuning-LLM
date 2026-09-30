from pyspark.sql import functions as F

filtered_payments = df_payments.filter(~F.col("method").isin(["cash", "promptpay", "bank_transfer"]))

agg = (
    filtered_payments
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.max("amount").alias("max_amount"),
    )
)

result = agg.orderBy(F.col("stddev_amount").desc())
