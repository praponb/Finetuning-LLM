from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .select("id", "plan", "status", F.last_day("end_date").alias("end_date_month_end"))
)
