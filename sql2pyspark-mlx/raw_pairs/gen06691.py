from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("amount") < 1500)

agg = (
    filtered_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"), "method")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = agg
