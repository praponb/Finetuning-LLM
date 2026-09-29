from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(F.max("amount").alias("max_amount"))
    .orderBy("paid_month")
)
