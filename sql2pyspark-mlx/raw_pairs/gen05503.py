from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "status", F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(F.max("end_date").alias("last_end_date"))
    .orderBy("last_end_date", F.col("end_month").desc(), "plan", "status")
    .limit(50)
)
