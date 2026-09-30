from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2024-07-01")
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(F.max("paid_at").alias("latest_paid_at"))
    .orderBy("latest_paid_at", "paid_day")
    .limit(5)
)
