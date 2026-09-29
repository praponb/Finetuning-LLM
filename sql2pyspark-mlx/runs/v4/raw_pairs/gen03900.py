from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.year("start_date") == 2025) | (F.col("start_date") < "2024-06-01"))
    .groupBy(F.year("end_date").alias("end_year"))
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
)
