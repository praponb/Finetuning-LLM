from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.year("end_date") == 2024) & (F.col("status") == "EXPIRED"))
    .groupBy("status", "plan")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .filter(F.col("max_monthly_fee") >= 140)
    .orderBy("max_monthly_fee")
)
