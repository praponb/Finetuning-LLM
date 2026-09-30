from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 610)
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(F.min("paid_at").alias("first_paid_at"))
    .orderBy("paid_month")
)
