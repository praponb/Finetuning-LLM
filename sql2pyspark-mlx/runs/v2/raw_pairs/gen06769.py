from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("paid_at") >= "2024-04-01")

agg = (
    filtered_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    agg
    .filter(F.col("max_amount") >= 780)
    .orderBy("avg_amount", "paid_month")
    .limit(1)
)
