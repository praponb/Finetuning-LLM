from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 210) & F.col("end_date").between("2024-01-23", "2025-03-01") & (F.col("monthly_fee") >= 120))
    .withColumn("start_date_period", F.date_trunc("MONTH", "start_date"))
)
