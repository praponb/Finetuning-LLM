from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("amount_rounded", F.round("amount", 2))
    .withColumn("months_since_paid_at", (F.datediff(F.current_date(), "paid_at") / 30))
)
