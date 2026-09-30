from pyspark.sql import functions as F

result = (
    df_subscriptions
    .cube(F.date_trunc("MONTH", "end_date").alias("end_month"), "plan")
    .agg(F.max("start_date").alias("latest_start_date"))
    .orderBy(F.col("end_month").desc())
)
