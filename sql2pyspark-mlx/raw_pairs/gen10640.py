from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("days_since_paid_at", F.datediff(F.current_date(), "paid_at"))
    .orderBy(F.col("method").desc())
)
